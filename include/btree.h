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
#ifndef _BTREE_H_
#define _BTREE_H_

#include "apiexp.h"
#include "nestalib.h"

#define KEY_FILE_EXT    ".nky"
#define DATA_FILE_EXT   ".ndt"

#define MAX_KEYSIZE 1024

struct btree_t {
    CS_DEF(critical_section);
    struct btkey_t* btkey;
    struct dio_data_t* btdat;
};

struct btk_element_t {
    char* key;          /* order * keysize */
    long dataptr;
};

struct btk_page_t {
    int keycount;
    struct btk_element_t* keytbl;   /* key[order] */
    int* child;                     /* child[order+1] */
};

struct btk_cache_element_t {
    int rpn;                        /* page number */
    struct btk_page_t* page;
    unsigned int refcnt;
};

struct btk_cache_t {
    struct btkey_t* btkey;
    int capacity;
    int count;
    struct btk_cache_element_t* cache_tbl;  /* [capacity] */
};

struct btkey_t {
    int fd;
    int pagesize;                   /* page size(4096, 8192, 16383, 32768) */
    int keysize;                    /* keysize */
    int order;                      /* order */
    int root;                       /* root page number */
    int free;                       /* free page number */
    int page_memsize;
    struct btk_cache_t* page_cache;
    struct btk_page_t* wkpage;
    char* pagebuf;                  /* read/write buffer(pagesize) */
    struct mmap_t* mmap;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* btree.c */
APIEXPORT struct btree_t* btopen(const char* filename, int key_cache_size);
APIEXPORT int btcreate(const char* filename, int keysize);
APIEXPORT void btclose(struct btree_t* bt);
APIEXPORT int btfile(const char* filename);
APIEXPORT int btput(struct btree_t* bt, const void* key, int keysize, const void* val, int valsize);
APIEXPORT int btsearch(struct btree_t* bt, const void* key, int keysize);
APIEXPORT int btget(struct btree_t* bt, const void* key, int keysize, void* val, int valsize);
APIEXPORT int btdelete(struct btree_t* bt, const void* key, int keysize);

/* btio.c */
struct btkey_t* btk_open(const char* fname, int cache_size);
int btk_create(const char* fname, ushort pagesize, ushort keysize, ushort order);
void btk_close(struct btkey_t* btkey);
int btk_put_root(struct btkey_t* btkey, int rpn);
int btk_avail_page(struct btkey_t* btkey);
int btk_read_page(struct btkey_t* btkey, int rpn, struct btk_page_t* keypage);
int btk_write_page(struct btkey_t* btkey, int rpn, struct btk_page_t* keypage);
int btk_delete_page(struct btkey_t* btkey, int rpn);

/* btcache.c */
struct btk_page_t* btk_alloc_page(struct btkey_t* btkey);
void btk_clear_page(struct btkey_t* btkey, struct btk_page_t* page);
void btk_page_copy(struct btkey_t* btkey, struct btk_page_t* dst, struct btk_page_t* src);
void btk_free_page(struct btk_page_t* page);
struct btk_cache_t* btk_cache_alloc(struct btkey_t* btkey, int count);
void btk_cache_free(struct btk_cache_t* c);
void btk_cache_copy(struct btk_cache_t* c, struct btk_page_t* dst, struct btk_page_t* src);
int btk_cache_get(struct btk_cache_t* c, int rpn, struct btk_page_t* page);
void btk_cache_set(struct btk_cache_t* c, int rpn, struct btk_page_t* page);
void btk_cache_update(struct btk_cache_t* c, int rpn, struct btk_page_t* page);
void btk_cache_delete(struct btk_cache_t* c, int rpn);

#ifdef __cplusplus
}
#endif

#endif /* _BTREE_H_ */
