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

#include "server.h"
#include "core.h"
#include "crawl.h"

// get a manifest on cache miss
// none of the blocks will have hashes; instead, we will serve signed blocks
// generate a wholly new manifest
// return 0 on success, and fill in *manifest
// return -ENOMEM on OOM
// return -ENOENT if the manifest is not present
static int AG_server_manifest_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_manifest* manifest, uint64_t hints, void* cls ) {

   int rc = 0;
   UG_handle_t* fh = NULL;
   struct md_entry ent_data;
   struct ms_client* ms = SG_gateway_ms( gateway );
   uint64_t max_block = 0;
   uint64_t new_version = 0;
   struct UG_inode* inode = NULL;
   struct UG_state* ug_core = (struct UG_state*)SG_gateway_cls( gateway );
   uint64_t block_size = ms_client_get_volume_blocksize( ms );
   struct timespec ts;

   memset( &ent_data, 0, sizeof(struct md_entry) );

   rc = UG_stat_raw( ug_core, reqdat->fs_path, &ent_data );
   if( rc != 0 ) {
      SG_error("UG_stat_raw('%s') rc = %d\n", reqdat->fs_path, rc );
      goto AG_server_manifest_get_end;
   }

   max_block = (ent_data.size / block_size);
   if( ent_data.size % block_size > 0 ) {
      max_block++;
   }

   md_entry_free( &ent_data );

   // pick version to be the current millisecond
   // TODO find something with higher entropy
   clock_gettime( CLOCK_REALTIME, &ts );
   new_version = ts.tv_sec * 1000 + (ts.tv_nsec / 1000000);

   fh = UG_open( ug_core, reqdat->fs_path, O_RDONLY, &rc );
   if( fh == NULL ) {
      SG_error("UG_open('%s') rc = %d\n", reqdat->fs_path, rc );
      goto AG_server_manifest_get_end;
   }

   rc = AG_crawl_blocks_reversion( ug_core, fh, 0, max_block, new_version );
   if( rc != 0 ) {
      SG_error("AG_crawl_blocks_reversion('%s' 0-%" PRIu64 ") rc = %d\n", reqdat->fs_path, max_block );
      goto AG_server_manifest_get_end;
   }

   // export manifest
   UG_handle_rlock( fh );
   inode = UG_handle_inode( fh );

   UG_inode_rlock( inode );
   rc = SG_manifest_dup( manifest, UG_inode_manifest( inode ) );
   UG_inode_unlock( inode );
   inode = NULL;

   UG_handle_unlock( fh );

   if( rc != 0 ) {
      SG_error("SG_manifest_dup rc = %d\n", rc );
      goto AG_server_manifest_get_end;
   }

AG_server_manifest_get_end:

   if( fh != NULL ) {
      rc = UG_close( ug_core, fh );
      fh = NULL;
      if( rc != 0 ) {
         SG_error("UG_close('%s') rc = %d\n", reqdat->fs_path, rc );
      }
   }

   return rc;
}


// get a block on cache miss (farm out to the driver)
// because we get blocks from upstream lazily, the resulting block will be a signed block
// return 0 on success, and fill in *block
// return -ENOMEM on OOM
// return -ENOENT if the block does not exist
// return -EIO if the driver did not fulfill the request (driver error)
// return -ENODATA if we couldn't request the data, for whatever reason (gateway error)
static int AG_server_block_get( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* block, uint64_t hints, void* cls ) {

   int rc = 0;
   int i = 0;
   int64_t worker_rc = 0;
   struct SG_chunk tmp_chunk;
   struct ms_client* ms = SG_gateway_ms( gateway );

   memset( &tmp_chunk, 0, sizeof(struct SG_chunk) );

   struct UG_state* ug_core = (struct UG_state*)SG_gateway_cls( gateway );

   UG_state_rlock( ug_core );

   struct AG_state* core = (struct AG_state*)UG_state_cls( ug_core );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   SG_messages::DriverRequest driver_req;

   char debug_block[21];
   bool debug_block_string = true;
   memset( debug_block, 0, 21 );

   AG_state_rlock( core );

   // find a reader
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "read" );
   if( group != NULL && SG_proc_group_size( group ) > 0 ) {

      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {

         // nothing running
         rc = -ENODATA;
         goto AG_server_block_get_finish;
      }

      // ask for the block
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_block_get_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_block_get_finish;
      }

      // get error code
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {

         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_block_get_finish;
      }

      // bail if the gateway had a problem
      if( worker_rc < 0 ) {

         SG_error("Request to worker %d failed, rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );

         if( worker_rc == -ENOENT ) {
             rc = -ENOENT;
         }
         else {
             rc = -EIO;
         }

         goto AG_server_block_get_finish;
      }

      // get the block
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), &tmp_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );

         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto AG_server_block_get_finish;
      }

      // sign the block
      rc = SG_client_block_sign( gateway, reqdat, &tmp_chunk, block );
      if( rc < 0 ) {

         SG_error("SG_gateway_block_sign(%" PRIu64 ") rc = %d\n", reqdat->block_id, rc );
         SG_chunk_free( &tmp_chunk );
         goto AG_server_block_get_finish;
      }

      memcpy( debug_block, tmp_chunk.data, MIN(tmp_chunk.len, 20) );
      SG_chunk_free( &tmp_chunk );

      // check special characters that may crash terminal
      for( i=0;i<20;i++ ) {
          if( debug_block[i] != 0 && debug_block[i] != '\n' && debug_block[i] != '\r' && !(debug_block[i] >= 33 && debug_block[i] <= 126) ) {
              // out of printable characters
              debug_block_string = false;
              break;
          }
      }
      if( debug_block_string ) {
          SG_debug( "Block data for %" PRIX64 "[%" PRIu64 ".%" PRId64 "]: '%s'...\n", reqdat->file_id, reqdat->block_id, reqdat->block_version, debug_block );
      }
   }
   else {

      // no way to do work--no process group
      rc = -ENODATA;
   }

AG_server_block_get_finish:

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }

   AG_state_unlock( core );
   UG_state_unlock( ug_core );
   return rc;
}


// gateway callback to deserialize a chunk
// return 0 on success, and fill in *chunk
// return -ENOMEM on OOM
// return -EIO if the driver did not fulfill the request (driver error)
// return -EAGAIN if we couldn't request the data, for whatever reason (i.e. no free processes)
int AG_server_chunk_deserialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk, void* cls ) {

   int rc = 0;
   int64_t worker_rc = 0;
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;
   struct SG_driver* driver = NULL;

   struct UG_state* ug_core = (struct UG_state*)SG_gateway_cls( gateway );

   UG_state_rlock( ug_core );

   struct AG_state* core = (struct AG_state*)UG_state_cls( ug_core );

   AG_state_rlock( core );

   // find a free deserializer
   driver = SG_gateway_driver( gateway );
   group = SG_driver_get_proc_group( driver, "deserialize" );
   if( group != NULL && SG_proc_group_size( group ) > 0 ) {

      // get a free process
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {

         // nothing running
         rc = -EAGAIN;
         goto AG_server_chunk_deserialize_finish;
      }

      // feed in the metadata for this block
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;
         goto AG_server_chunk_deserialize_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;
         goto AG_server_chunk_deserialize_finish;
      }

      // feed in the block itself
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), in_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin(proc), rc );

         rc = -EIO;
         goto AG_server_chunk_deserialize_finish;
      }

      // get error code
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {

         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_chunk_deserialize_finish;
      }

      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );

      // bail if the driver had a problem
      if( worker_rc < 0 ) {

         SG_error("Worker %d: deserialize rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;

         goto AG_server_chunk_deserialize_finish;
      }

      // get the serialized chunk
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), out_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno( SG_proc_stdout_f(proc) ), rc );

         // OOM, EOF, or driver crash (rc is -ENOMEM, -ENODATA, or -EIO, respectively)
         goto AG_server_chunk_deserialize_finish;
      }
   }
   else {

      // no-op deserializer
      rc = SG_chunk_dup( out_chunk, in_chunk );
   }

AG_server_chunk_deserialize_finish:

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }

   if( rc != 0 ) {
      SG_chunk_free( out_chunk );
   }

   AG_state_unlock( core );
   UG_state_unlock( ug_core );
   return rc;
}


// gateway callback to serialize a chunk
// return 0 on success
// return -ENOMEM on OOM
// return -EIO if we failed to communicate with the driver (i.e. driver error)
// return -EAGAIN if there were no free workers
int AG_server_chunk_serialize( struct SG_gateway* gateway, struct SG_request_data* reqdat, struct SG_chunk* in_chunk, struct SG_chunk* out_chunk, void* cls ) {

   int rc = 0;
   int64_t worker_rc = 0;
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   struct SG_driver* driver = NULL;
   struct ms_client* ms = SG_gateway_ms( gateway );
   SG_messages::DriverRequest driver_req;

   struct UG_state* ug_core = (struct UG_state*)SG_gateway_cls( gateway );

   UG_state_rlock( ug_core );

   struct AG_state* core = (struct AG_state*)UG_state_cls( ug_core );

   AG_state_rlock( core );

   // find a worker
   driver = SG_gateway_driver( gateway );
   group = SG_driver_get_proc_group( driver, "serialize" );
   if( group != NULL && SG_proc_group_size( group ) > 0 ) {

      // get a free worker
      proc = SG_proc_group_acquire( group );
      if( proc == NULL ) {

         // no free workers
         rc = -EAGAIN;
         goto AG_server_chunk_serialize_finish;
      }

      // feed in the metadata for this block
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;
         goto AG_server_chunk_serialize_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;
         goto AG_server_chunk_serialize_finish;
      }

      // put the block
      rc = SG_proc_write_chunk( SG_proc_stdin( proc ), in_chunk );
      if( rc < 0 ) {

         SG_error("SG_proc_write_chunk(%d) rc = %d\n", SG_proc_stdin( proc ), rc );

         rc = -EIO;
         goto AG_server_chunk_serialize_finish;
      }

      // get the reply
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {

         SG_error("SG_proc_read_int64(%d) rc = %d\n", fileno(SG_proc_stdout_f( proc )), rc );

         rc = -EIO;
         goto AG_server_chunk_serialize_finish;
      }

      SG_debug("Worker rc = %" PRId64 "\n", worker_rc );

      if( worker_rc < 0 ) {

         SG_error("Worker %d: serialize rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );
         rc = -EIO;

         goto AG_server_chunk_serialize_finish;
      }

      // get the deserialized chunk
      rc = SG_proc_read_chunk( SG_proc_stdout_f( proc ), out_chunk );
      if( rc != 0 ) {

         SG_error("SG_proc_read_chunk(%d) rc = %d\n", fileno(SG_proc_stdout_f(proc)), rc );
         goto AG_server_chunk_serialize_finish;
      }
   }
   else {

      // no-op serializer
      rc = SG_chunk_dup( out_chunk, in_chunk );
   }

AG_server_chunk_serialize_finish:

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }

   AG_state_unlock( core );
   UG_state_unlock( ug_core );
   return rc;
}


// refresh a file on remote request
// simply forward the request to the driver, and if the driver decides
// that the file still exists, it will ask the AG to re-crawl it (or
// delete it if it doesn't exist).
// return 0 on success
// return -ENOMEM on OOM
static int AG_server_refresh( struct SG_gateway* gateway, struct SG_request_data* reqdat, void* cls ) {

   int rc = 0;
   int64_t worker_rc = 0;
   struct ms_client* ms = SG_gateway_ms( gateway );
   struct UG_state* ug_core = (struct UG_state*)SG_gateway_cls( gateway );

   UG_state_rlock( ug_core );

   struct AG_state* core = (struct AG_state*)UG_state_cls( ug_core );
   struct SG_proc_group* group = NULL;
   struct SG_proc* proc = NULL;
   SG_messages::DriverRequest driver_req;

   AG_state_rlock( core );

   // find a refresh
   group = SG_driver_get_proc_group( SG_gateway_driver(gateway), "crawl" );
   if( group != NULL && SG_proc_group_size( group ) > 0 ) {

      // get a free process
      proc = SG_proc_group_timed_acquire( group, -1, &rc );
      if( proc == NULL ) {

         // nothing running
         SG_debug("No free processes in the 'crawl' group: SG_proc_group_timed_acquire rc = %d\n", rc);
         rc = -ENODATA;
         goto AG_server_block_get_finish;
      }

      // ask for the file
      rc = SG_proc_request_init( ms, reqdat, &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_request_init rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_block_get_finish;
      }

      rc = SG_proc_write_request( SG_proc_stdin( proc ), &driver_req );
      if( rc != 0 ) {

         SG_error("SG_proc_write_request rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_block_get_finish;
      }

      // get error code
      rc = SG_proc_read_int64( SG_proc_stdout_f( proc ), &worker_rc );
      if( rc < 0 ) {

         SG_error("SG_proc_read_int64('ERROR') rc = %d\n", rc );
         rc = -EIO;

         goto AG_server_block_get_finish;
      }

      // bail if the gateway had a problem
      if( worker_rc < 0 ) {

         SG_error("Request to worker %d failed, rc = %d\n", SG_proc_pid( proc ), (int)worker_rc );

         if( worker_rc == -ENOENT ) {
             rc = -ENOENT;
         }
         else {
             rc = -EIO;
         }

         goto AG_server_block_get_finish;
      }
   }
   else {

      SG_debug("Refresh request on '%s' (%" PRIX64 ".%" PRId64 ")\n", reqdat->fs_path, reqdat->file_id, reqdat->file_version );
      rc = 0;
   }

AG_server_block_get_finish:

   if( group != NULL && proc != NULL ) {
      SG_proc_group_release( group, proc );
   }

   AG_state_unlock( core );
   UG_state_unlock( ug_core );
   return rc;
}



// set up the gateway's method implementation
// always succeeds
int AG_server_install_methods( struct SG_gateway* gateway ) {

   // disable UG implementations
   SG_impl_connect_cache( gateway, NULL );
   SG_impl_truncate( gateway, NULL );
   SG_impl_rename( gateway, NULL );
   SG_impl_detach( gateway, NULL );
   SG_impl_patch_manifest( gateway, NULL ); // TODO: reenable once we have write support?

   // enable AG implementations
   SG_impl_get_block( gateway, AG_server_block_get );
   SG_impl_get_manifest( gateway, AG_server_manifest_get );

   SG_impl_serialize( gateway, AG_server_chunk_serialize );
   SG_impl_deserialize( gateway, AG_server_chunk_deserialize );

   // allow UGs to ask for refreshes
   SG_impl_refresh( gateway, AG_server_refresh );

   return 0;
}
