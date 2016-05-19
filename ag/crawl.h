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

#ifndef _AG_CRAWL_H_
#define _AG_CRAWL_H_

#include <libsyndicate/libsyndicate.h>
#include <libsyndicate-ug/client.h>
#include <libsyndicate-ug/core.h>

#include <fskit/fskit.h>

#define AG_CRAWL_CMD_CREATE  'C'    // create, but fail if the entry exists
#define AG_CRAWL_CMD_PUT     'P'    // create-or-update
#define AG_CRAWL_CMD_UPDATE  'U'    // update, but fail if the entry doesn't exist
#define AG_CRAWL_CMD_DELETE  'D'    // delete, but fail if the entry doesn't exist
#define AG_CRAWL_CMD_FINISH  'F'    // indicates that there are no more datasets to crawl

// indexes into a single stanza
#define AG_CRAWL_STANZA_CMD 0
#define AG_CRAWL_STANZA_MD  1
#define AG_CRAWL_STANZA_PATH 2
#define AG_CRAWL_STANZA_TERM 3

#define AG_CRAWL_STANZA_TERM_STR "0"

// we read stanzas form the AG driver
// in a non-blocking manner.  This structure
// buffers up partially-read stanzas.
struct AG_stanza {

   // stanza state
   int state;
#define AG_STANZA_STATE_NEW   0
#define AG_STANZA_STATE_CONT  1
#define AG_STANZA_STATE_READY 2
#define AG_STANZA_STATE_INVAL 3
   int sock_fd;

   // stanza buffers
#define AG_STANZA_LENBUF_SIZE 32
   char lenbuf[AG_STANZA_LENBUF_SIZE+1];
   char* lines[4];       // point to offsets in linebuf
   char* linebuf;
   int off;
   int size;

   // parsed data 
   int cmd;
   struct md_entry data;
   char* path;
};

typedef map<pid_t, struct AG_stanza> AG_stanza_set_t;


extern "C" {

int AG_stanza_set_new( AG_stanza_set_t* stanzas, pid_t pid, int input_fd );
int AG_stanza_set_has_pid( AG_stanza_set_t* stanzas, pid_t pid );
int AG_stanza_set_make_fdset( AG_stanza_set_t* stanzas, fd_set* fds );
int AG_stanza_set_remove_badfds( AG_stanza_set_t* stanzas );
int AG_crawl_consume_stanzas( struct AG_state* ag, AG_stanza_set_t* stanzas, fd_set* readable_fds );
int AG_crawl_blocks_reversion( struct UG_state* ug, UG_handle_t* h, uint64_t block_id_start, uint64_t block_id_end, int64_t version );

}
#endif
