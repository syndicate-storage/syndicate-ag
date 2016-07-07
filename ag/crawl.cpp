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

/*
 Crawl format is a four-line stanza:
 * command string
 * metadata string
 * path string
 * terminator string

 command string is a 1-character, newline-terminated line:
 * C for 'create'
 * U for 'update'
 * P for 'put'
 * D for 'delete'

 mode string is a two-field newline-terminated line:
 * "$type $mode $size", where
    * $type is 'D' for directory or 'F' for file
    * $mode is the octal mode
    * $size is the size of the file

 path string is a well-formed absolute path; accepted as-is minus the newline

 terminator string is a null character, and a newline.
*/

#include "crawl.h"
#include "core.h"

// copy a mode-string and path-string into an md_entry
// only the type, mode, and name will be set.  Everything else will be left as-is
// return 0 on success
// return -EINVAL on bad input
// return -ENOMEM on OOM
static int AG_crawl_parse_metadata( char const* md_linebuf, char* path_linebuf, struct md_entry* data ) {

   int rc = 0;

   char type = 0;
   mode_t mode = 0;
   uint64_t size = 0;
   int32_t max_read_freshness = 0;
   int32_t max_write_freshness = 0;

   rc = sscanf( md_linebuf, "%c 0%o %" PRIu64 " %d %d", &type, &mode, &size, &max_read_freshness, &max_write_freshness );
   if( rc != 5 ) {
      SG_error("Invalid mode string '%s'\n", md_linebuf );
      return -EINVAL;
   }

   if( type != 'D' && type != 'F' ) {
      SG_error("Invalid mode string type '%c'\n", type );
      return -EINVAL;
   }

   data->type = (type == 'D' ? MD_ENTRY_DIR : MD_ENTRY_FILE);
   data->mode = mode & 0666;    // force read-only for now
   data->size = size;
   data->name = md_basename( path_linebuf, NULL );
   data->max_read_freshness = max_read_freshness;
   data->max_write_freshness = max_write_freshness;

   if( data->name == NULL ) {
      return -ENOMEM;
   }

   SG_debug("Parsed (%c, %s, 0%o, %" PRIu64 ", read_ttl=%d, write_ttl=%d)\n", type, data->name, data->mode, data->size, data->max_read_freshness, data->max_write_freshness );

   return 0;
}


// obtain the crawl command from a crawl command string
// return 0 on success, and set *cmd
// return -EINVAL on bad input
static int AG_crawl_parse_command( char const* cmd_linebuf, int* cmd ) {

   int rc = 0;
   char cmd_type = 0;

   rc = sscanf( cmd_linebuf, "%c", &cmd_type );
   if( rc != 1 ) {
      SG_error("Invalid command string '%s'\n", cmd_linebuf );
      return -EINVAL;
   }

   if( cmd_type != AG_CRAWL_CMD_CREATE &&
       cmd_type != AG_CRAWL_CMD_PUT &&
       cmd_type != AG_CRAWL_CMD_UPDATE &&
       cmd_type != AG_CRAWL_CMD_DELETE &&
       cmd_type != AG_CRAWL_CMD_FINISH ) {

      SG_error("Invalid command '%c'\n", cmd_type );
      return -EINVAL;
   }

   *cmd = cmd_type;
   return 0;
}


// parse the stanza from its linebuf
// return 0 on success
// return -ENOMEM on OOM
// return -EINVAL on invalid data
static int AG_stanza_parse( struct AG_stanza* stanza ) {

   int rc = 0;
   char* tmp = NULL;
   char* tok = NULL;
   char* str = stanza->linebuf;
   int num_lines = 0;

   // break linebuf into lines
   while( 1 ) {
      tok = strtok_r( str, "\n", &tmp );
      str = NULL;

      if( tok == NULL ) {
         // out of lines
         break;
      }
      if( strlen(tok) == 0 ) {
         // out of lines
         break;
      }

      SG_debug("Line: '%s'\n", tok );
      stanza->lines[num_lines] = tok;
      num_lines++;

      if( num_lines > 4 ) {
         // too many lines
         SG_error("Too many lines in stanza: %d\n", num_lines);
         rc = -EINVAL;
         break;
      }
   }

   if( rc < 0 ) {
      return rc;
   }

   // last line must be terminator
   if( strcmp(stanza->lines[AG_CRAWL_STANZA_TERM], AG_CRAWL_STANZA_TERM_STR) != 0 ) {
      SG_error("Invalid terminator: '%s'\n", stanza->lines[AG_CRAWL_STANZA_TERM] );
      return rc;
   }

   // parse cmd
   rc = AG_crawl_parse_command( stanza->lines[AG_CRAWL_STANZA_CMD], &stanza->cmd );
   if( rc < 0 ) {
      SG_error("AG_crawl_parse_command rc = %d\n", rc );
      return rc;
   }

   // parse metadata
   rc = AG_crawl_parse_metadata( stanza->lines[AG_CRAWL_STANZA_MD], stanza->lines[AG_CRAWL_STANZA_PATH], &stanza->data );
   if( rc < 0 ) {
      SG_error("AG_crawl_parse_metadata rc = %d\n", rc );
      return rc;
   }

   stanza->path = stanza->lines[AG_CRAWL_STANZA_PATH];
   return 0;
}


// free a stanza
int AG_stanza_free( struct AG_stanza* stanza ) {

   if( stanza->linebuf != NULL ) {
      SG_safe_free( stanza->linebuf );
      stanza->linebuf = NULL;
      memset( stanza->lines, 0, sizeof(char*) * 4 );
   }

   if( stanza->sock_fd >= 0 ) {
      close(stanza->sock_fd);
      stanza->sock_fd = -1;
   }

   md_entry_free( &stanza->data );

   return 0;
}

// read the size, and if we succeed, allocate the linebuf and copy over the carryover
// return 0 on success
// return 1 to try again
// return -EPERM on failure
// does not set the stanza's status
static int AG_stanza_linebuf_setup( int fd, struct AG_stanza* stanza ) {

   int rc = 0;
   ssize_t nr = 0;
   int len = 0;
   char* carryover_start = NULL;

   nr = recv( fd, stanza->lenbuf + stanza->off, AG_STANZA_LENBUF_SIZE - stanza->off, 0 );
   if( nr < 0 ) {
      rc = -errno;
      SG_error("recv(%d) rc = %d\n", fd, rc );
      return -EPERM;
   }
   if( nr == 0 ) {
      // EOF
      SG_error("recv(%d): EOF\n", fd );
      return -EPERM;
   }

   SG_debug("Stanza %p: read %zd linebuf length bytes (offset %jd, at %d)\n", stanza, nr, stanza->off, stanza->off + nr );
   stanza->off += nr;

   // read size and colon?
   rc = sscanf( stanza->lenbuf, "%d:", &len );
   if( rc != 1 ) {
      // not done reading yet
      return 1;
   }

   SG_debug("Stanza %p: %d-byte line buffer\n", stanza, len );

   // allocate linebuf
   stanza->linebuf = SG_CALLOC( char, len + 1 );
   if( stanza->linebuf == NULL ) {
      return -EPERM;
   }

   stanza->size = len;

   // copy in remnant
   carryover_start = strchr( stanza->lenbuf, ':' );
   if( carryover_start == NULL ) {
      // no remnant (should never happen)
      SG_error("BUG: failed to fine carryover in %p\n", stanza->lenbuf);
      exit(1);
   }

   carryover_start++;
   strcpy( stanza->linebuf, carryover_start );
   SG_debug("Stanza %p: carried over %zu bytes\n", stanza, strlen(stanza->linebuf) );

   // clear lenbuf
   stanza->off = strlen(stanza->linebuf);
   memset( stanza->lenbuf, 0, AG_STANZA_LENBUF_SIZE+1 );

   return 0;
}

// read a stanza from a file stream, and populate an AG_stanza
// return 0 on success
// return 1 if we aren't done yet (EAGAIN)
// return -EINVAL if we got invalid data
// return -ENOMEM on OOM
// return -EBADF if the process is dead
// return -EPERM if the request failed for any other reason
// Recovery will be attempted by reading ahead to the next terminating string (if one is not found immediately)
static int AG_crawl_read_stanza( int fd, struct AG_stanza* stanza ) {

   int rc = 0;
   ssize_t nr = 0;

   if( stanza->state == AG_STANZA_STATE_INVAL ) {
      SG_debug("Stanza %p is in state INVAL\n", stanza );
      return -EINVAL;
   }

   if( stanza->state == AG_STANZA_STATE_READY ) {
      SG_debug("Stanza %p is in state READY\n", stanza);
      return 0;
   }

   if( stanza->state == AG_STANZA_STATE_NEW ) {

      // clear everything but the input file descriptor
      fd = stanza->sock_fd;
      memset( stanza, 0, sizeof(struct AG_stanza) );
      stanza->sock_fd = fd;

      stanza->state = AG_STANZA_STATE_CONT;
      SG_debug("Stanza %p now in state CONT\n", stanza );
   }

   if( stanza->state == AG_STANZA_STATE_CONT ) {

      if( stanza->linebuf == NULL ) {

         // set up linebuf
         SG_debug("Stanza %p: set up linebuf\n", stanza);
         rc = AG_stanza_linebuf_setup( fd, stanza );
         if( rc < 0 ) {
            SG_error("AG_stanza_linebuf_setup rc = %d\n", rc );
            SG_debug("Stanza %p now in state INVAL\n", stanza );
            stanza->state = AG_STANZA_STATE_INVAL;
            return -EPERM;
         }

         else if( rc > 0 ) {
            // try again
            return 1;
         }
      }

      // read linebuf data
      nr = recv( fd, stanza->linebuf + stanza->off, stanza->size - stanza->off, 0 );
      if( nr <= 0 ) {

         if( nr == 0 ) {
            // EOF
            SG_error("recv(%d): EOF\n", fd );
            SG_debug("Stanza %p now in state INVAL\n", stanza );
            stanza->state = AG_STANZA_STATE_INVAL;
            return -EPERM;
         }

         rc = -errno;
         if( rc == -EAGAIN ) {
            // consumed data; keep going
            return 1;
         }
         else {
             SG_error("recv(%d) rc = %d\n", fd, rc );
             SG_debug("Stanza %p now in state INVAL\n", stanza );
             stanza->state = AG_STANZA_STATE_INVAL;
             return -EPERM;
         }
      }

      SG_debug("Stanza %p consumed %zd bytes (%jd out of %d)\n", stanza, nr, stanza->off + nr, stanza->size );

      stanza->off += nr;
      if( stanza->off >= stanza->size ) {
         // got all the data!
         // parse it
         rc = AG_stanza_parse( stanza );
         if( rc < 0 ) {
            SG_error("AG_stanza_parse rc = %d\n", rc );
            SG_debug("Stanza %p now in state INVAL\n", stanza );
            stanza->state = AG_STANZA_STATE_INVAL;
            return -EPERM;
         }

         // parsed!
         SG_debug("Stanza %p now in state READY\n", stanza );
         stanza->state = AG_STANZA_STATE_READY;
         return 0;
      }
      else {

         // have more to go
         return 1;
      }
   }

   // invalid state
   SG_error("BUG: invalid state %d\n", stanza->state );
   return -EINVAL;
}


// set the version for a range of blocks
// return 0 on success
// return negative on error
int AG_crawl_blocks_reversion( struct UG_state* ug, UG_handle_t* h, uint64_t block_id_start, uint64_t block_id_end, int64_t version ) {

   int rc = 0;

   for( uint64_t i = block_id_start; i <= block_id_end; i++ ) {

      SG_debug("Put block info %" PRIu64 "\n", i );
      rc = UG_putblockinfo( ug, i, version, NULL, h );
      if( rc != 0 ) {
         SG_error("UG_putblockinfo(%" PRIu64 ") rc = %d\n", i, rc );
         break;
      }
   }

   return rc;
}


// handle a 'create' command
// return 0 on success
// return -ENOMEM on OOM
// return -EPERM on failure to execute the operation
// return -EACCES on permission error
// return -EEXIST if the requested entry already exists
// return -ENOENT if a parent directory does not exist
// return -EREMOTEIO on all other errors
static int AG_crawl_create( struct AG_state* core, char const* path, struct md_entry* ent ) {

   int rc = 0;
   struct SG_gateway* gateway = AG_state_gateway( core );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = AG_state_ug( core );
   struct timespec now;
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   uint64_t num_blocks = 0;
   int close_rc = 0;
   UG_handle_t* h = NULL;

   ent->file_id = ms_client_make_file_id();
   clock_gettime( CLOCK_REALTIME, &now );
 
   ent->mtime_sec = now.tv_sec;
   ent->mtime_nsec = now.tv_nsec;
   ent->ctime_sec = now.tv_sec;
   ent->ctime_nsec = now.tv_nsec;

   // try to create or mkdir
   if( ent->type == MD_ENTRY_FILE ) {
       clock_gettime( CLOCK_REALTIME, &now );

       ent->manifest_mtime_sec = now.tv_sec;
       ent->manifest_mtime_nsec = now.tv_nsec;
      
       h = UG_publish( ug, path, ent, &rc );
       if( h == NULL ) {
          SG_error("UG_publish(%s) rc = %d\n", path, rc );
          goto AG_crawl_create_out;
       }

       // fill in manifest block info: block id, block version (but not hash)
       num_blocks = (ent->size / block_size) + 1;

       rc = AG_crawl_blocks_reversion( ug, h, 0, num_blocks, 1 );
       if( rc != 0 ) {
          SG_error("AG_crawl_blocks_reversion(%s[%" PRIu64 "-%" PRIu64 "], %" PRId64 ") rc = %d\n",
                path, (uint64_t)0, num_blocks, (uint64_t)1, rc );
       }

       close_rc = UG_close( ug, h );
       if( close_rc != 0 ) {
          SG_error("UG_close(%s) rc = %d\n", path, close_rc );
       }

       h = NULL;
   }
   else {
      ent->size = 4096;     // default directory size
      rc = UG_publish_dir( ug, path, ent->mode, ent );
      if( rc != 0 ) {
         SG_error("UG_publish_dir(%s) rc = %d\n", path, rc );
         goto AG_crawl_create_out;
      }
   }

AG_crawl_create_out:

   if( rc != 0 && rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -EEXIST && rc != -ENOENT ) {
       rc = -EREMOTEIO;
   }
   return rc;
}


// handle an 'update' command
// * reversion each block that already existed (i.e. on a size increase, reversion the blocks affecting bytes <= size)
// * add blocks for new data (on size increase)
// * if the size decreased, truncate the file
// * post new metadata to the MS
// if forced_reversion is true, then all blocks will be reversioned
// This method will go and fetch the previous inode's metadata.
// return 0 on success
// return -ENOENT if the entry does not exist on the MS
// return -EACCES if we're not allowed to read it
// return -EREMOTEIO on failure to communicate with the MS
static int AG_crawl_update( struct AG_state* core, char const* path, struct md_entry* ent, bool force_reversion ) {

   int rc = 0;
   struct SG_gateway* gateway = AG_state_gateway( core );
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug = AG_state_ug( core );
   struct SG_gateway* gw = UG_state_gateway( ug );
   struct md_syndicate_cache* cache = SG_gateway_cache( gw );
   struct timespec now;
   struct SG_client_WRITE_data* update = NULL;
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   int close_rc = 0;
   UG_handle_t* h = NULL;
   struct md_entry prev_ent;
   uint64_t new_block_id_start = 0;
   uint64_t num_blocks = 0;
   int64_t max_version = 0;
   int64_t tmp_version = 0;
   bool clear_cache = false;    // only do this if we're missing block info (means that we're starting up--any cached data will be stale)

   memset( &prev_ent, 0, sizeof(prev_ent) );

   if( ent->type == MD_ENTRY_FILE ) {

      // see how we differ
      h = UG_open( ug, path, O_RDONLY, &rc );
      if( h == NULL ) {
         SG_error("UG_open('%s') rc = %d\n", path, rc );
         goto AG_crawl_update_out;
      }

      rc = UG_stat_raw( ug, path, &prev_ent );
      if( rc != 0 ) {
         SG_error("UG_stat_raw('%s') rc = %d\n", path, rc );
         goto AG_crawl_update_out;
      }

      if( prev_ent.size < ent->size || force_reversion ) {

         // got bigger, or we're reversioning everything anyway
         // make new blocks
         if( force_reversion ) {
            new_block_id_start = 0;
         }
         else {
            new_block_id_start = prev_ent.size / block_size;
         }

         num_blocks = (ent->size / block_size);
         if( ent->size % block_size != 0 ) {
            num_blocks ++;
         }

         SG_debug("\n\nReversion %" PRIu64 "-%" PRIu64 "; clear %" PRIX64 ".%" PRId64"\n\n", new_block_id_start, num_blocks, prev_ent.file_id, prev_ent.version );

         // maximum block version so far...
         for( uint64_t i = new_block_id_start; i <= num_blocks; i++ ) {

            rc = UG_getblockinfo( ug, i, &tmp_version, NULL, h );
            if( rc != 0 ) {
               if( rc != -ENOENT ) {
                  SG_error("UG_getblockinfo(%" PRIu64 ") rc = %d\n", i, rc );
                  goto AG_crawl_update_out;
               }

               // can only happen if we're starting up
               clear_cache = true;
            }
            else {
               if( tmp_version > max_version ) {
                  max_version = tmp_version;
               }
            }
         }

         if( clear_cache ) {

            // clear old cached state for the file
            // TODO: block-granularity
            md_cache_clear_file( cache, prev_ent.file_id, prev_ent.version, 0 );
         }

         rc = AG_crawl_blocks_reversion( ug, h, new_block_id_start, num_blocks, max_version + 1 );
         if( rc != 0 ) {
            SG_error("AG_crawl_blocks_reversion(%s[%" PRIu64 "-%" PRIu64 "] %" PRId64 ") rc = %d\n", path, new_block_id_start, num_blocks, max_version + 1, rc );
            goto AG_crawl_update_out;
         }
      }
      if( prev_ent.size > ent->size ) {

         // shrank
         // mark manifest as fresh (so truncate doesn't go try to fetch it)
         // then we'll re-generate it ourselves
         /*
         UG_handle_rlock( h );

         inode = UG_handle_inode( h );

         UG_inode_wlock( inode );
         UG_inode_set_read_stale( inode, false );
         UG_inode_set_refresh_time_now( inode );
         UG_inode_unlock( inode );

         UG_handle_unlock( h );
         */

         // truncate
         rc = UG_ftruncate( ug, ent->size, h );
         if( rc != 0 ) {
            SG_error("UG_truncate('%s', %" PRIu64 ") rc = %d\n", path, ent->size, rc );
            goto AG_crawl_update_out;
         }

         // already updated on the MS, so nothing more to do
         goto AG_crawl_update_out;
      }
   }

   // generate the metadata update...
   update = SG_client_WRITE_data_new();
   if( update == NULL ) {
      rc = -ENOMEM;
      goto AG_crawl_update_out;
   }

   clock_gettime( CLOCK_REALTIME, &now );

   SG_client_WRITE_data_set_mtime( update, &now );
   SG_client_WRITE_data_set_mode( update, ent->mode );
   SG_client_WRITE_data_set_owner_id( update, ent->owner );
   SG_client_WRITE_data_set_size( update, ent->size );
   SG_client_WRITE_data_set_refresh( update, ent->max_read_freshness, ent->max_write_freshness );

   rc = UG_update( ug, path, update );

   SG_safe_free( update );

   if( rc != 0 ) {

      SG_error("UG_update(%s) rc = %d\n", path, rc );
      goto AG_crawl_update_out;
   }

AG_crawl_update_out:

   md_entry_free( &prev_ent );
   if( rc != 0 && rc != -EPERM && rc != -ENOMEM && rc != -ENOENT && rc != -EACCES ) {
      rc = -EREMOTEIO;
   }

   if( h != NULL ) {
      close_rc = UG_close( ug, h );
      if( close_rc != 0 ) {
         SG_error("UG_close('%s') rc = %d\n", path, close_rc );
      }
   }

   return rc;
}


// handle a put (a create-or-update)
// try to create, and if it fails with EEXIST, then send as an update instead.
// return 0 on success
// return -ENOMEM on OOM
// return -EPERM if the operation could not be completed
// return -EACCES if we don't have permission to create or update
// return -ENOENT if the parent directory doesn't exist
// return -EREMOTEIO on failure to communicate with the MS
static int AG_crawl_put( struct AG_state* core, char const* path, struct md_entry* ent ) {

   int rc = 0;

   SG_debug("Put '%s'\n", path);

   rc = AG_crawl_create( core, path, ent );
   if( rc == 0 ) {
      SG_debug("Created '%s'\n", path);
      return 0;
   }
   else if( rc == -EEXIST ) {

      // file already exists.
      // force reversion
      ent->version ++;

      // try to update, but update all blocks too
      rc = AG_crawl_update( core, path, ent, true );
      if( rc != 0 ) {
         SG_error("AG_crawl_update('%s') rc = %d\n", path, rc );
      }
      else {
        SG_debug("Updated '%s'\n", path);
      }
   }
   else {
     SG_error("AG_crawl_create('%s') rc = %d\n", path, rc );
   }

   return rc;
}


// handle a delete
// return 0 on success
// return -ENOMEM on OOM
// return -EPERM if the operation could not be completed
// return -EACCES if we don't have permission to delete this
// return -ENOENT if the entry doesn't exist
// return -EREMOTEIO on failure to communicate with the MS
static int AG_crawl_delete( struct AG_state* core, char const* path, struct md_entry* ent ) {

   int rc = 0;
   struct UG_state* ug = AG_state_ug( core );

   rc = UG_rmtree( ug, path );
   if( rc != 0 ) {

      SG_error("UG_rmdir(%s) rc = %d\n", path, rc );
      if( rc != -ENOMEM && rc != -EPERM && rc != -EACCES && rc != -ENOENT ) {
          rc = -EREMOTEIO;
      }
   }

   return rc;
}


// handle one crawl command
// return 0 on success
// return 1 if there are no more commands to be had
// return -ENOMEM on OOM
// return -ENOENT if we requested an update or delete on a non-existant entry
// return -EEXIST if we tried to create an entry that already existed
// return -EACCES on permission error
// return -EPERM on operation error
// return -EREMOTEIO on failure to communicate with the MS
int AG_crawl_process( struct AG_state* core, int cmd, char const* path, struct md_entry* ent ) {

   int rc = 0;
   struct SG_gateway* gateway = AG_state_gateway( core );
   struct ms_client* ms = SG_gateway_ms( gateway );

   // enforce these...
   ent->coordinator = SG_gateway_id( gateway );
   ent->volume = ms_client_get_volume_id( ms );

   switch( cmd ) {
      case AG_CRAWL_CMD_CREATE: {

         rc = AG_crawl_create( core, path, ent );
         if( rc != 0 ) {
            SG_error("AG_crawl_create(%s) rc = %d\n", path, rc );
         }

         break;
      }

      case AG_CRAWL_CMD_UPDATE: {

         rc = AG_crawl_update( core, path, ent, false );
         if( rc != 0 ) {
             SG_error("AG_crawl_update(%s) rc = %d\n", path, rc );
         }

         break;
      }

      case AG_CRAWL_CMD_PUT: {

         rc = AG_crawl_put( core, path, ent );
         if( rc  != 0 ) {
             SG_error("AG_crawl_put(%s) rc = %d\n", path, rc );
         }

         break;
      }

      case AG_CRAWL_CMD_DELETE: {

         rc = AG_crawl_delete( core, path, ent );
         if( rc != 0 ) {
             SG_error("AG_crawl_delete(%s) rc = %d\n", path, rc );
         }

         break;
      }

      case AG_CRAWL_CMD_FINISH: {
         rc = 1;
         break;
      }

      default: {
         SG_error("Unknown command type '%c'\n", cmd );
         rc = -EINVAL;
         break;
      }
   }

   return rc;
}


// put a new stanza for a process
// return 0 on success
// return -ENOMEM on OOM
int AG_stanza_set_new( AG_stanza_set_t* stanzas, pid_t pid, int sock_fd ) {

   struct AG_stanza new_stanza;
   int flags = 0;
   int rc = 0;

   memset( &new_stanza, 0, sizeof(struct AG_stanza) );

   new_stanza.state = AG_STANZA_STATE_NEW;
   new_stanza.sock_fd = sock_fd;

   // don't block
   flags = fcntl( sock_fd, F_GETFL );
   if( !(flags & O_NONBLOCK) ) {
      rc = fcntl( sock_fd, F_SETFL, flags | O_NONBLOCK );
      if( rc != 0 ) {
         rc = -errno;
         SG_error("fcntl(%d, O_NONBLOCK) rc = %d\n", sock_fd, rc );
         return -EPERM;
      }
   }

   try {
      (*stanzas)[pid] = new_stanza;
   }
   catch( bad_alloc& ba ) {
      return -ENOMEM;
   }

   return 0;
}


// is a PID already present in a stanza set?
// return 1 if so
// return 0 if not
int AG_stanza_set_has_pid( AG_stanza_set_t* stanzas, pid_t pid ) {
   if( stanzas->find(pid) != stanzas->end() ) {
      return 1;
   }
   else {
      return 0;
   }
}


// build an FDSET out of stanzas
// return maxfd
int AG_stanza_set_make_fdset( AG_stanza_set_t* stanzas, fd_set* fds ) {

   int sock = 0;
   int maxfd = 0;

   FD_ZERO( fds );
   for( auto itr = stanzas->begin(); itr != stanzas->end(); itr++ ) {
      sock = itr->second.sock_fd;
      FD_SET(sock, fds);
      maxfd = (sock > maxfd ? sock : maxfd);
   }

   return maxfd;
}


// remove all stanzas with remotely-closed sockets
// return 0 on success
int AG_stanza_set_remove_badfds( AG_stanza_set_t* stanzas ) {

   int rc = 0;
   int sock = 0;

   for( auto itr = stanzas->begin(); itr != stanzas->end(); ) {

      sock = itr->second.sock_fd;
      rc = fcntl( sock, F_GETFL );
      if( rc < 0 ) {
         rc = -errno;
         SG_error("Stanza fd %d is bad (rc = %d)\n", sock, rc );

         // clear it
         auto old_itr = itr;
         itr++;

         AG_stanza_free( &old_itr->second );
         stanzas->erase( old_itr );
      }
      else {

         itr++;
      }
   }

   return 0;
}


// consume incoming stanzas from running crawl processes.
// Update the given stanza set.
// If a stanza is completed, then process it and remove it from the stanza set.
// return 0 on success
int AG_crawl_consume_stanzas( struct AG_state* core, AG_stanza_set_t* stanzas, fd_set* readable_fds ) {

   int rc = 0;
   int64_t result = 0;

   pid_t pid = 0;
   struct AG_stanza* stanza = NULL;

   for( auto itr = stanzas->begin(); itr != stanzas->end(); ) {

      pid = itr->first;
      stanza = &itr->second;

      // readaable?
      if( !FD_ISSET( stanza->sock_fd, readable_fds ) ) {
         continue;
      }

      // consume stanza data
      rc = AG_crawl_read_stanza( stanza->sock_fd, stanza );
      if( rc > 0 ) {
         // not done yet
         itr++;
         rc = 0;
         continue;
      }

      else if( rc < 0 ) {
         // error
         SG_error("AG_crawl_read_stanza rc = %d\n", rc );
         result = -EIO;
         rc = 0;
      }
      else {
          // got a stanza!
          // process stanza
          SG_debug("Process stanza from proc %d\n", (int)pid );
          rc = AG_crawl_process( core, stanza->cmd, stanza->path, &stanza->data );
          result = rc;
          if( rc < 0 ) {
             SG_error("AG_crawl_process(%s) (%d) rc = %d\n", stanza->path, pid, rc );
          }
      }

      // reply the status
      rc = SG_proc_write_int64( stanza->sock_fd, result );
      if( rc < 0 ) {
         // will remove anyway
         SG_error("SG_proc_write_int64(%d) rc = %d\n", stanza->sock_fd, rc );
      }

      // done with stanza
      auto old_itr = itr;
      itr++;

      SG_debug("Done with stanza from proc %d\n", (int)pid);
      AG_stanza_free(stanza);
      stanzas->erase(old_itr);

      rc = 0;
   }

   return rc;
}
