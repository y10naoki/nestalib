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
#ifndef _FCACHE_H_
#define _FCACHE_H_

#include "apiexp.h"
#include "nestalib.h"

struct cache_element_t {
    char fpath[MAX_PATH];   /* full path */
    char* data;             /* file data pointer */
    time_t ts;              /* last update */
    int fsize;              /* data size(bytes) */
    int ref_c;              /* reference count */
    int del_flag;           /* delete flag */
};

struct file_cache_t {
    CS_DEF(cache_critical_section);
    int capacity;
    int count;
    int max_cache_size;
    int cur_cache_size;
    struct cache_element_t* element_list;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct file_cache_t* fc_initialize(int cache_size);
APIEXPORT void fc_finalize(struct file_cache_t* fc);
APIEXPORT char* fc_get(struct file_cache_t* fc, const char* fpath, time_t ts, int fsize);
APIEXPORT int fc_set(struct file_cache_t* fc, const char* fpath, time_t ts, int fsize, const char* data);

#ifdef __cplusplus
}
#endif

#endif /* _FCACHE_H_ */
