/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2008-2011 YAMAMOTO Naoki
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
#ifndef _HDB_H_
#define _HDB_H_

#include "nestalib.h"

/* hash database */
struct hdb_t {
    CS_DEF(critical_section);
    struct nio_t* nio;              /* (stuct nio_t*) */
    HASH_FUNCPTR hash_func;         /* hash func */
    CMP_FUNCPTR cmp_func;           /* compare func */
    int bucket_num;                 /* bucket number */
    size_t mmap_view_size;          /* mmap view size */
    int fd;                         /* file pointer */
    ushort align_bytes;             /* key data align bytes */
    int filling_rate;               /* filling rate(%) */
};

/* The implemented function is as follows.
    hdb_cursor_open()
    hdb_cursor_close()
    hdb_cursor_next()
    hdb_cursor_key()
 */
struct hdbcursor_t {
    struct hdb_t* hdb;              /* hash datatbase object */
    int bucket_index;               /* bucket index (>=0) */
    int64 kvptr;                    /* struct hdb_keyvalue_t pointer */
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* hdb.c */
struct hdb_t* hdb_initialize(struct nio_t* nio);
void hdb_finalize(struct hdb_t* hdb);
void hdb_cmpfunc(struct hdb_t* hdb, CMP_FUNCPTR func);
void hdb_hashfunc(struct hdb_t* hdb, HASH_FUNCPTR func);
int hdb_property(struct hdb_t* hdb, int kind, int value);
int hdb_open(struct hdb_t* hdb, const char* fname);
int hdb_create(struct hdb_t* hdb, const char* fname);
void hdb_close(struct hdb_t* hdb);
int hdb_file(const char* fname);
int hdb_find(struct hdb_t* hdb, const void* key, int keysize);
int hdb_get(struct hdb_t* hdb, const void* key, int keysize, void* val, int valsize);
int hdb_gets(struct hdb_t* hdb, const void* key, int keysize, void* val, int valsize, int64* cas);
void* hdb_aget(struct hdb_t* hdb, const void* key, int keysize, int* valsize);
void* hdb_agets(struct hdb_t* hdb, const void* key, int keysize, int* valsize, int64* cas);
int hdb_put(struct hdb_t* hdb, const void* key, int keysize, const void* val, int valsize);
int hdb_puts(struct hdb_t* hdb, const void* key, int keysize, const void* val, int valsize, int64 cas);
int hdb_bset(struct hdb_t* hdb, const void* key, int keysize, const void* val, int valsize, int64 cas);
int hdb_delete(struct hdb_t* hdb, const void* key, int keysize);
void hdb_free(const void* v);

/* cursor I/O */
struct hdbcursor_t* hdb_cursor_open(struct hdb_t* bdb);
void hdb_cursor_close(struct hdbcursor_t* cur);
int hdb_cursor_next(struct hdbcursor_t* cur);
int hdb_cursor_key(struct hdbcursor_t* cur, void* key, int keysize);

#ifdef __cplusplus
}
#endif

#endif /* _HDB_H_ */
