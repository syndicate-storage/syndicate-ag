/*
   Copyright 2016 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "syndicate-ag.h"
#include "core.h"
#include "crawl.h"

// global running flag
volatile bool g_running = true;

// toggle running flag for the crawler loop
void AG_set_running( bool running ) {
   g_running = running;
}


// find the list of crawlers by scanning the IPC directory 
// return the number of processes on success
// return -ERANGE if *pids isn't big enough
// return -errno on directory-related failures
static int AG_crawl_find_crawl_processes( struct AG_state* ag, pid_t* pids, size_t num_pids ) {

   int rc = 0;
   int idx = 0;
   int num_dents = 0;
   pid_t pid = 0;
   struct stat sb;
   char ipc_dir_buf[PATH_MAX+1];
   char sock_path[PATH_MAX+20];
   struct UG_state* ug = AG_state_ug( ag );
   struct SG_gateway* gateway = UG_state_gateway( ug );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );

   struct dirent** names = NULL;
   
   char* ipc_dir = md_conf_get_ipc_dir( conf, ipc_dir_buf, PATH_MAX+1 );
   if( ipc_dir == NULL ) {
      SG_error("%s", "FATAL: overflow\n");
      exit(1);
   }

   rc = scandir( ipc_dir, &names, NULL, alphasort );
   if( rc < 0 ) {
      rc = -errno;
      SG_error("scandir('%s') rc = %d\n", ipc_dir, rc );
      return rc;
   }
   
   num_dents = rc;

   if( (unsigned)num_dents > num_pids ) {
      // not enough space
      idx = -ERANGE;
      goto AG_crawl_find_crawl_processes_out;
   }

   for( int i = 0; i < num_dents; i++ ) {
      
      // has to start with a number and end in '.sock' 
      rc = sscanf( names[i]->d_name, "%d.sock", &pid );
      if( rc != 1 ) {
         continue;
      }

      rc = snprintf(sock_path, PATH_MAX+20, "%s/%s", ipc_dir, names[i]->d_name );
      if( rc == PATH_MAX+20 ) {
         // name too long
         continue;
      }

      // is this a socket?
      rc = stat( sock_path, &sb );
      if( rc < 0 ) {
         SG_debug("stat('%s') rc = %d\n", sock_path, rc );
         continue;
      }

      if( !S_ISSOCK(sb.st_mode) ) {
         SG_debug("Not a socket: '%s'\n", sock_path);
         continue;
      }

      // a PID!
      pids[idx] = pid;
      idx++;
   }

AG_crawl_find_crawl_processes_out:

   if( names != NULL ) {
      for( int i = 0; i < num_dents; i++ ) {
         free(names[i]);
      }
      free(names);
   }

   return idx;
}

// try to connect to an AG crawler.
// Use exponential back-off if need be, until the driver sandbox
// creates the UNIX domain socket for us to use to receive datasets.
// Return the socket descriptor on success.
// Return -errno on failure.
static int AG_crawl_connect( struct AG_state* ag, pid_t pid ) {

   int rc = 0;
   int timeout = 1;
   struct timespec sleeptime;
   struct stat sb;
   char ipc_dir_buf[PATH_MAX+1];
   char socket_path[PATH_MAX+1];
   struct UG_state* ug = AG_state_ug( ag );
   struct SG_gateway* gateway = UG_state_gateway( ug );
   struct md_syndicate_conf* conf = SG_gateway_conf( gateway );
   struct sockaddr_un connection_addr;
   int sock = 0;

   char* ipc_dir = md_conf_get_ipc_dir( conf, ipc_dir_buf, PATH_MAX+1 );
   if( ipc_dir == NULL ) {
      SG_error("%s", "FATAL: overflow\n");
      exit(1);
   }

   // search for this process's UNIX domain socket 
   rc = snprintf( socket_path, PATH_MAX+1, "%s/%d.sock", ipc_dir, pid );
   if( rc == PATH_MAX+1 ) {
      SG_error("%s", "FATAL: overflow\n");
      exit(1);
   }

   while( 1 ) {
      // if it doesn't exist, wait for a bit 
      rc = stat(socket_path, &sb);
      if( rc != 0 ) {
         rc = -errno;
         if( rc == -ENOENT ) {
             
            timeout = timeout * 2 + (md_random32() % timeout);
            if( timeout > 1000 ) {
               timeout = 1000;
            }
            
            SG_debug("No such file or directory: '%s'.  Sleeping %d milliseconds and trying again\n", socket_path, timeout );
            sleeptime.tv_nsec = timeout * 100000;
            sleeptime.tv_sec = 0;
            nanosleep( &sleeptime, NULL );
            continue;
         }
         else {
            SG_error("stat('%s') rc = %d\n", socket_path, rc );
            return rc;
         }
      }

      // is this a socket?
      if( !S_ISSOCK( sb.st_mode )) {
         SG_error("Not a socket: '%s'\n", socket_path );
         return -ENOENT;
      }

      // found!
      break;
   }

   // connect!
   sock = socket( AF_UNIX, SOCK_STREAM, 0 );
   if( sock < 0 ) {
      rc = -errno;
      SG_error("socket() rc = %d\n", rc );
      return rc;
   }

   memset( &connection_addr, 0, sizeof(struct sockaddr_un) );
   connection_addr.sun_family = AF_UNIX;
   strncpy( connection_addr.sun_path, socket_path, sizeof(connection_addr.sun_path) - 1 );

   rc = connect( sock, (const struct sockaddr*)&connection_addr, sizeof(struct sockaddr_un) );
   if( rc < 0 ) {
      rc = -errno;
      SG_error("connect('%s') rc = %d\n", socket_path, rc );
      close(sock);
      return rc;
   }

   return sock;
}


// AG main loop: crawl the dataset, using the crawl process
// receive commands from it. 
static void* AG_crawl_loop( void* cls ) {

   int rc = 0;
   int sock = 0;
   int maxfd = 0;
   int num_crawlers = 0;
   struct AG_state* ag = (struct AG_state*)cls;

   fd_set sockets;
   AG_stanza_set_t stanzas;
   pid_t crawl_pids[1024];

   // select for 1 second 
   struct timeval select_interval;
   select_interval.tv_sec = 1;
   select_interval.tv_usec = 0;

   while( g_running ) {

      // connect to any crawlers 
      num_crawlers = AG_crawl_find_crawl_processes( ag, crawl_pids, 1024 );
      if( num_crawlers < 0 ) {
         SG_error("AG_crawl_find_crawl_processes rc = %d\n", num_crawlers );
         sleep(1);
         continue;
      }

      // connect to any new crawlers 
      for( int i = 0; i < num_crawlers; i++ ) {
         if( AG_stanza_set_has_pid( &stanzas, crawl_pids[i] ) ) {
            continue;
         }

         // new crawler!
         sock = AG_crawl_connect( ag, crawl_pids[i] );
         if( sock < 0 ) {
            SG_error("AG_crawl_connect rc = %d\n", sock);
            continue;
         }

         rc = AG_stanza_set_new( &stanzas, crawl_pids[i], sock );
         if( rc != 0 ) {
            SG_error("FATAL: AG_stanza_set_new rc = %d\n", rc );
            break;
         }
      }

      maxfd = AG_stanza_set_make_fdset( &stanzas, &sockets );
      
      // sleep for at most 1 second
      select_interval.tv_sec = 1;
      select_interval.tv_usec = 0;
      maxfd = select( maxfd+1, &sockets, NULL, NULL, &select_interval );
      
      if( maxfd < 0 ) {
         rc = -errno;
         if( rc == -EBADF ) {

            // clear out entries with bad file descriptors  
            AG_stanza_set_remove_badfds( &stanzas );

            // try again 
            continue;
         }
      }

      if( maxfd == 0 ) {
         // no data 
         continue;
      }

      // someone has data
      // consume them
      AG_crawl_consume_stanzas( ag, &stanzas, &sockets );
   }

   SG_debug("%s", "Crawler thread exit\n");
   return NULL;
}


// entry point 
int main( int argc, char** argv ) {

   int rc = 0;
   int exit_code = 0;
   struct AG_state* ag = NULL;
   pthread_t crawl_thread;

   // setup...
   ag = AG_init( argc, argv );
   if( ag == NULL ) {
      
      SG_error("%s", "AG_init failed\n" );
      exit(1);
   }

   // start crawler 
   rc = md_start_thread( &crawl_thread, AG_crawl_loop, ag, false );
   if( rc != 0 ) {
      SG_error("md_start_thread rc = %d\n", rc );
      exit(1);
   }

   // run gateway 
   rc = AG_main( ag );
   if( rc != 0 ) {
      SG_error("AG_main rc = %d\n", rc );
      exit_code = 1;
   }

   // stop crawler 
   g_running = false;
   pthread_cancel( crawl_thread );
   pthread_join( crawl_thread, NULL );

   // stop gateway 
   rc = AG_shutdown( ag );
   if( rc != 0 ) {
      SG_error("AG_shutdown rc = %d\n", rc );
   }

   SG_safe_free( ag );
   exit(exit_code);
}

