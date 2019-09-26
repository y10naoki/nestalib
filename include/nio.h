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
#ifndef _NIO_H_
#define _NIO_H_

#include "apiexp.h"
#include "nestalib.h"

/* dbtype */
#define NIO_HASH        1       /* hash database */
#define NIO_BTREE       2       /* B+tree database */

/* kind of property */
#define NIO_BUCKET_NUM      1   /* number (only hash) */
#define NIO_PAGESIZE        2   /* bytes (only B+tree) */
#define NIO_MAP_VIEWSIZE    3   /* MB */
#define NIO_ALIGN_BYTES     4   /* bytes */
#define NIO_FILLING_RATE    5   /* ratio(%) */
#define NIO_DUPLICATE_KEY   6   /* duplicates key(1 or 0)(only B+tree) */
#define NIO_DATAPACK        7   /* packed key & data(1 or 0)(only B+tree) */
#define NIO_PREFIX_COMPRESS 8   /* prefix compress key(1 or 0)(only B+tree) */

#define NIO_MAX_KEYSIZE     1024

#define NIO_FREEPAGE_SIZE   4096
#define NIO_FREE_COUNT      ((NIO_FREEPAGE_SIZE-16)/12)

#define NIO_FREEDATA_OFFSET         16

#define NIO_FREEPAGE_ID             0xCCEE

#define NIO_FREEPAGE_NEXT_OFFSET    2
#define NIO_FREEPAGE_COUNT_OFFSET   14
#define NIO_FREEPAGE_ARRAY_OFFSET   16

/* free data block id */
#define NIO_FREEDATA_ID             0xDDEE

#define NIO_CURSOR_END  1

/* compare function API */
typedef int (*CMP_FUNCPTR)(const void * key1, int key1size, const void* key2, int key2size);

/* hash function API */
typedef unsigned int (*HASH_FUNCPTR)(const void * key, int len, unsigned int seed);

#include "bdb.h"
#include "hdb.h"

/* databese function API */
typedef void (*FINALIZE_FUNCPTR)(void* db);
typedef int (*PROPERTY_FUNCPTR)(void* db, int kind, int value);
typedef int (*OPEN_FUNCPTR)(void* db, const char* fname);
typedef int (*CREATE_FUNCPTR)(void* db, const char* fname);
typedef void (*CLOSE_FUNCPTR)(void* db);
typedef int (*FILE_FUNCPTR)(const char* fname);
typedef int (*FIND_FUNCPTR)(void* db, const void* key, int keysize);
typedef int (*GET_FUNCPTR)(void* db, const void* key, int keysize, void* val, int valsize);
typedef int (*GETS_FUNCPTR)(void* db, const void* key, int keysize, void* val, int valsize, int64* cas);
typedef void* (*AGET_FUNCPTR)(void* db, const void* key, int keysize, int* valsize);
typedef void* (*AGETS_FUNCPTR)(void* db, const void* key, int keysize, int* valsize, int64* cas);
typedef int (*PUT_FUNCPTR)(void* db, const void* key, int keysize, const void* val, int valsize);
typedef int (*PUTS_FUNCPTR)(void* db, const void* key, int keysize, const void* val, int valsize, int64 cas);
typedef int (*BSET_FUNCPTR)(void* db, const void* key, int keysize, const void* val, int valsize, int64 cas);
typedef int (*DELETE_FUNCPTR)(void* db, const void* key, int keysize);
typedef void (*FREE_FUNCPTR)(const void* v);

/* cursor function API */
typedef void* (*CURSOR_OPEN_FUNCPTR)(void* db);
typedef void (*CURSOR_CLOSE_FUNCPTR)(void* cur);
typedef int (*CURSOR_NEXT_FUNCPTR)(void* cur);
typedef int (*CURSOR_NEXTKEY_FUNCPTR)(void* cur);
typedef int (*CURSOR_PREV_FUNCPTR)(void* cur);
typedef int (*CURSOR_PREVKEY_FUNCPTR)(void* cur);
typedef int (*CURSOR_DUPLICATE_LAST_FUNCPTR)(void* cur);
typedef int (*CURSOR_FIND_FUNCPTR)(void* cur, int cond, const void* key, int keysize);
typedef int (*CURSOR_SEEK_FUNCPTR)(void* cur, int pos);
typedef int (*CURSOR_KEY_FUNCPTR)(void* cur, void* key, int keysize);
typedef int (*CURSOR_VALUE_FUNCPTR)(void* cur, void* val, int valsize);
typedef int (*CURSOR_UPDATE_FUNCPTR)(void* cur, const void* val, int valsize);
typedef int (*CURSOR_DELETE_FUNCPTR)(void* cur);

struct nio_free_t {
    int64 offset;
    int count;
    int page_size[NIO_FREE_COUNT];
    int64 data_ptr[NIO_FREE_COUNT];
    int64 next_ptr;
};

struct nio_t {
    int dbtype;                     /* database type */
    int64 free_ptr;                 /* free area pointer */
    struct nio_free_t* free_page;
    struct mmap_t* mmap;
    void* db;                       /* struct hdb_t*|struct bdb_t* */

    /* function pointer */
    FINALIZE_FUNCPTR finalize_func;
    PROPERTY_FUNCPTR property_func;
    OPEN_FUNCPTR open_func;
    CREATE_FUNCPTR create_func;
    CLOSE_FUNCPTR close_func;
    FILE_FUNCPTR file_func;
    FIND_FUNCPTR find_func;
    GET_FUNCPTR get_func;
    GETS_FUNCPTR gets_func;
    AGET_FUNCPTR aget_func;
    AGETS_FUNCPTR agets_func;
    PUT_FUNCPTR put_func;
    PUTS_FUNCPTR puts_func;
    BSET_FUNCPTR bset_func;
    DELETE_FUNCPTR delete_func;
    FREE_FUNCPTR free_func;

    /* cursor function pointer */
    CURSOR_OPEN_FUNCPTR cursor_open_func;
    CURSOR_CLOSE_FUNCPTR cursor_close_func;
    CURSOR_NEXT_FUNCPTR cursor_next_func;
    CURSOR_NEXTKEY_FUNCPTR cursor_nextkey_func;
    CURSOR_PREV_FUNCPTR cursor_prev_func;
    CURSOR_PREVKEY_FUNCPTR cursor_prevkey_func;
    CURSOR_DUPLICATE_LAST_FUNCPTR cursor_duplicate_last_func;
    CURSOR_FIND_FUNCPTR cursor_find_func;
    CURSOR_SEEK_FUNCPTR cursor_seek_func;
    CURSOR_KEY_FUNCPTR cursor_key_func;
    CURSOR_VALUE_FUNCPTR cursor_value_func;
    CURSOR_UPDATE_FUNCPTR cursor_update_func;
    CURSOR_DELETE_FUNCPTR cursor_delete_func;
};

struct nio_cursor_t {
    int dbtype;                     /* database type */
    struct nio_t* nio;              /* database object */
    void* cursor;                   /* hdb(struct hdbcursor_t*) /
                                       bdb(struct dbcursor_t*) */
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* nio.c */
int nio_cmpkey(const void* k1, int k1size, const void* k2, int k2size);
char* nio_make_filename(char* fpath, const char* basename, const char* extname);
int64 nio_filesize(struct nio_t* nio);
int nio_create_free_page(struct nio_t* nio);
int nio_add_free_list(struct nio_t* nio, int64 ptr, int size);
int64 nio_avail_space(struct nio_t* nio, int size, int* areasize, int filling_rate);

struct nio_t* nio_initialize(int dbtype);
void nio_finalize(struct nio_t* nio);
void nio_cmpfunc(struct nio_t* nio, CMP_FUNCPTR func);
void nio_hashfunc(struct nio_t* nio, HASH_FUNCPTR func);
int nio_property(struct nio_t* nio, int kind, int value);
int nio_open(struct nio_t* nio, const char* fname);
int nio_create(struct nio_t* nio, const char* fname);
void nio_close(struct nio_t* nio);
int nio_file(struct nio_t* nio, const char* fname);
int nio_find(struct nio_t* nio, const void* key, int keysize);
int nio_get(struct nio_t* nio, const void* key, int keysize, void* val, int valsize);
int nio_gets(struct nio_t* nio, const void* key, int keysize, void* val, int valsize, int64* cas);
void* nio_aget(struct nio_t* nio, const void* key, int keysize, int* valsize);
void* nio_agets(struct nio_t* nio, const void* key, int keysize, int* valsize, int64* cas);
int nio_put(struct nio_t* nio, const void* key, int keysize, const void* val, int valsize);
int nio_puts(struct nio_t* nio, const void* key, int keysize, const void* val, int valsize, int64 cas);
int nio_bset(struct nio_t* nio, const void* key, int keysize, const void* val, int valsize, int64 cas);
int nio_delete(struct nio_t* nio, const void* key, int keysize);
void nio_free(struct nio_t* nio, const void* v);

/* cursor I/O */
struct nio_cursor_t* nio_cursor_open(struct nio_t* nio);
void nio_cursor_close(struct nio_cursor_t* cur);
int nio_cursor_next(struct nio_cursor_t* cur);
int nio_cursor_nextkey(struct nio_cursor_t* cur);
int nio_cursor_prev(struct nio_cursor_t* cur);
int nio_cursor_prevkey(struct nio_cursor_t* cur);
int nio_cursor_duplicate_last(struct nio_cursor_t* cur);
int nio_cursor_find(struct nio_cursor_t* cur, int cond, const void* key, int keysize);
int nio_cursor_seek(struct nio_cursor_t* cur, int pos);
int nio_cursor_key(struct nio_cursor_t* cur, void* key, int keysize);
int nio_cursor_value(struct nio_cursor_t* cur, void* val, int valsize);
int nio_cursor_update(struct nio_cursor_t* cur, const void* val, int valsize);
int nio_cursor_delete(struct nio_cursor_t* cur);

#ifdef __cplusplus
}
#endif

#endif /* _NIO_H_ */
