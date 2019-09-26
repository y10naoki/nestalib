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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define API_INTERNAL
#include "nestalib.h"

struct btk_page_t* btk_alloc_page(struct btkey_t* btkey)
{
    struct btk_page_t* page;
    char* p;
    int i;

    /* insert_key() のために order + 1 で確保する。
       最後の要素は insert_key() 用の一時領域とする。*/
    page = (struct btk_page_t*)calloc(1, btkey->page_memsize);
    if (page == NULL) {
        err_write("btk_alloc_page: no memory.");
        return NULL;
    }

    page->keytbl = (struct btk_element_t*)(page+1);
    p = (char*)page->keytbl + (btkey->order+1) * sizeof(struct btk_element_t);

    for (i = 0; i < btkey->order+1; i++) {
        page->keytbl[i].key = p;
        p += btkey->keysize;
    }

    page->child = (int*)p;
    return page;
}

void btk_clear_page(struct btkey_t* btkey, struct btk_page_t* page)
{
    int i;

    page->keycount = 0;
    for (i = 0; i < btkey->order; i++) {
        memset(page->keytbl[i].key, '\0', btkey->keysize);
        page->keytbl[i].dataptr = 0;
    }
    memset(page->child, '\0', (btkey->order+1) * sizeof(int));
}

void btk_page_copy(struct btkey_t* btkey, struct btk_page_t* dst, struct btk_page_t* src)
{
    int i;

    dst->keycount = src->keycount;
    memcpy(dst->keytbl->key, src->keytbl->key, btkey->order * btkey->keysize);
    for (i = 0; i < src->keycount; i++)
        dst->keytbl[i].dataptr = src->keytbl[i].dataptr;
    memcpy(dst->child, src->child, (btkey->order+1) * sizeof(int));
}

void btk_free_page(struct btk_page_t* page)
{
    if (page != NULL)
        free(page);
}

struct btk_cache_t* btk_cache_alloc(struct btkey_t* btkey, int count)
{
    struct btk_cache_t* c;
    int i;

    c = (struct btk_cache_t*)calloc(1, sizeof(struct btk_cache_t));
    if (c == NULL) {
        err_write("cache_alloc: no memory.");
        return NULL;
    }
    c->cache_tbl = (struct btk_cache_element_t*)calloc(count, sizeof(struct btk_cache_element_t));
    if (c->cache_tbl == NULL) {
        err_write("cache_alloc: no memory count=%d.", count);
        free(c);
        return NULL;
    }

    c->btkey = btkey;
    c->capacity = count;

    for (i = 0; i < count; i++) {
        c->cache_tbl[i].page = btk_alloc_page(btkey);
        if (c->cache_tbl[i].page == NULL) {
            err_write("cache_alloc: no memory count=%d.", count);
            btk_cache_free(c);
            return NULL;
        }
    }
    return c;
}

void btk_cache_free(struct btk_cache_t* c)
{
    if (c != NULL) {
        if (c->cache_tbl != NULL) {
            int i;

            for (i = 0; i < c->capacity; i++) {
                if (c->cache_tbl[i].page != NULL)
                    btk_free_page(c->cache_tbl[i].page);
            }
            free(c->cache_tbl);
        }
        free(c);
    }
}

void btk_cache_copy(struct btk_cache_t* c, struct btk_page_t* dst, struct btk_page_t* src)
{
    btk_page_copy(c->btkey, dst, src);
}

int btk_cache_get(struct btk_cache_t* c, int rpn, struct btk_page_t* page)
{
    if (c != NULL) {
        int i;

        for (i = 0; i < c->count; i++) {
            if (c->cache_tbl[i].rpn == rpn) {
                /* found */
                btk_cache_copy(c, page, c->cache_tbl[i].page);
                c->cache_tbl[i].refcnt++;
                return 1;
            }
        }
    }
    return 0;   /* not found in cache */
}

static int cache_out(struct btk_cache_t* c)
{
    int i;
    int index = -1;
    unsigned int minref = 0xffffffff;

    /* 参照カウントの少ないページをキャッシュアウトの対象とします。*/
    for (i = 0; i < c->count; i++) {
        if (c->cache_tbl[i].refcnt < minref) {
            minref = c->cache_tbl[i].refcnt;
            index = i;
        }
    }
    return index;
}

void btk_cache_set(struct btk_cache_t* c, int rpn, struct btk_page_t* page)
{
    if (c != NULL) {
        int index;

        if (c->count < c->capacity) {
            /* キャッシュに空きがあるので追加 */
            index = c->count++;
        } else {
            /* キャッシュアウトするページを決めます。*/
            index = cache_out(c);
        }

        if (index >= 0) {
            c->cache_tbl[index].rpn = rpn;
            btk_cache_copy(c, c->cache_tbl[index].page, page);
            c->cache_tbl[index].refcnt = 0;
        }
    }
}

static int is_cachein(struct btk_cache_t* c, int rpn)
{
    int i;

    for (i = 0; i < c->count; i++) {
        if (c->cache_tbl[i].rpn == rpn)
            return i;   /* found */
    }
    return -1;  /* notfound */
}

void btk_cache_update(struct btk_cache_t* c, int rpn, struct btk_page_t* page)
{
    if (c != NULL) {
        int index;

        index = is_cachein(c, rpn);
        if (index >= 0)
            btk_cache_copy(c, c->cache_tbl[index].page, page);
    }
}

void btk_cache_delete(struct btk_cache_t* c, int rpn)
{
    if (c != NULL) {
        int index;

        index = is_cachein(c, rpn);
        if (index >= 0) {
            c->count--;
            if (index < c->count) {
                int i;

                for (i = index; i < c->count; i++) {
                    c->cache_tbl[i].rpn = c->cache_tbl[i+1].rpn;
                    btk_cache_copy(c, c->cache_tbl[i].page, c->cache_tbl[i+1].page);
                    c->cache_tbl[i].refcnt = c->cache_tbl[i+1].refcnt;
                }
            }
            btk_clear_page(c->btkey, c->cache_tbl[c->count].page);
        }
    }
}
