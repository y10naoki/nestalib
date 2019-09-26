/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2008-2013 YAMAMOTO Naoki
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#ifndef _BDB_H_
#define _BDB_H_

#include "nestalib.h"

#define BDB_PACK_DATASIZE   255     /* max packed data size */
#define BDB_MAX_PREFIX_SIZE 255     /* max prefix size */

#define BDB_COND_EQ         0
#define BDB_COND_GT         1
#define BDB_COND_GE         2
#define BDB_COND_LT         3
#define BDB_COND_LE         4

#define BDB_SEEK_TOP        0
#define BDB_SEEK_BOTTOM     1

/* leaf header flag */
#define PREFIX_COMPRESS_NODE        0x01

/* leaf node struct */
struct bdb_leaf_t {
    int64 node_ptr;
    int keynum;
    int nodesize;                   /* node size(include header size) */
    int64 next_ptr;
    int64 prev_ptr;
    uchar flag;                     /* 2013/09/08 add compress node flag */
};

struct bdb_value_t {
    int areasize;                   /* area size */
    int valsize;                    /* value size */
    int64 next_ptr;                 /* next of duplicate key */
    int64 prev_ptr;                 /* prev of duplicate key */
};

struct bdb_leaf_value_t {
    union {
        struct {
            int valsize;
            uchar val[BDB_PACK_DATASIZE];
        } pp;
        struct {
            int64 v_ptr;
        } dp;
    } u;
};

struct bdb_leaf_key_t {
    int keysize;
    uchar key[NIO_MAX_KEYSIZE];
    struct bdb_leaf_value_t value;
};

struct bdb_slot_t {
    int index;
    union {
        struct {
            int valsize;
            uchar val[BDB_PACK_DATASIZE];
        } pp;
        struct {
            int64 v_ptr;
            struct bdb_value_t v;
        } dp;
    } u;
};

/* leaf cache */
struct leaf_cache_t {
    struct bdb_leaf_t leaf;
    int alloc_keys;
    struct bdb_leaf_key_t* keydata;
    int update;
};

/* B+tree */
struct bdb_t {
    CS_DEF(critical_section);
    struct nio_t* nio;                  /* (stuct nio_t*) */
    CMP_FUNCPTR cmp_func;               /* compare func */
    int node_pgsize;                    /* node page size */
    size_t mmap_view_size;              /* mmap view size */
    int fd;                             /* fd */
    ushort fver;                        /* file version */
    ushort align_bytes;                 /* key data align bytes */
    int filling_rate;                   /* filling rate(%) */
    int64 root_ptr;                     /* root */
    int64 leaf_top_ptr;                 /* top of leaf */
    int64 leaf_bot_ptr;                 /* last of leaf */
    int dupkey_flag;                    /* enable duplicate key */
    int datapack_flag;                  /* packed data flag */
    char* node_buf;                     /* node I/O buffer(node_pgsize) */
    char* leaf_buf;                     /* leaf I/O buffer(node_pgsize) */
    struct leaf_cache_t* leaf_cache;    /* leaf I/O cache */
    int64 filesize;                     /* file size */
    int prefix_compress_flag;           /* enable prefix compress */
};

/* cursor struct */
struct dbcursor_t {
    struct bdb_t* bdb;
    int64 node_ptr;                     /* current leaf node pointer */
    int index;                          /* in bdb->leaf_cache->keydata */
    struct bdb_slot_t slot;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* bdb.c */
struct bdb_t* bdb_initialize(struct nio_t* nio);
void bdb_finalize(struct bdb_t* bdb);
void bdb_cmpfunc(struct bdb_t* bdb, CMP_FUNCPTR func);
int bdb_property(struct bdb_t* bdb, int kind, int value);
int bdb_open(struct bdb_t* bdb, const char* fname);
int bdb_create(struct bdb_t* bdb, const char* fname);
void bdb_close(struct bdb_t* bdb);
int bdb_file(const char* fname);
int bdb_find(struct bdb_t* bdb, const void* key, int keysize);
int bdb_get(struct bdb_t* bdb, const void* key, int keysize, void* val, int valsize);
void* bdb_aget(struct bdb_t* bdb, const void* key, int keysize, int* valsize);
int bdb_put(struct bdb_t* bdb, const void* key, int keysize, const void* val, int valsize);
int bdb_delete(struct bdb_t* bdb, const void* key, int keysize);
void bdb_free(const void* v);

/* cursor I/O */
struct dbcursor_t* bdb_cursor_open(struct bdb_t* bdb);
void bdb_cursor_close(struct dbcursor_t* cur);
int bdb_cursor_next(struct dbcursor_t* cur);
int bdb_cursor_nextkey(struct dbcursor_t* cur);
int bdb_cursor_prev(struct dbcursor_t* cur);
int bdb_cursor_prevkey(struct dbcursor_t* cur);
int bdb_cursor_duplicate_last(struct dbcursor_t* cur);
int bdb_cursor_find(struct dbcursor_t* cur, int cond, const void* key, int keysize);
int bdb_cursor_seek(struct dbcursor_t* cur, int pos);
int bdb_cursor_key(struct dbcursor_t* cur, void* key, int keysize);
int bdb_cursor_value(struct dbcursor_t* cur, void* val, int valsize);
int bdb_cursor_update(struct dbcursor_t* cur, const void* val, int valsize);
int bdb_cursor_delete(struct dbcursor_t* cur);

#ifdef __cplusplus
}
#endif

#endif /* _BDB_H_ */
