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
#ifndef _POOL_H_
#define _POOL_H_

#include "csect.h"
#include "apiexp.h"

#define POOL_NOTIMEOUT (-1)
#define POOL_NOWAIT    0

typedef void* (*CALLBACK_POOL_ADD)(void*);
typedef void (*CALLBACK_POOL_REMOVE)(void*);

struct pool_element_t {
    void* data;             /* data pointer */
    int used;               /* using flag */
    int64 systime;          /* start time(usec) */
    int64 last_access;      /* last access(usec) */
};

struct pool_t {
    CS_DEF(critical_section);
    int init_num;
    int capacity;
    int element_num;
    struct pool_element_t* e;
    long timeout_ms;
    CALLBACK_POOL_ADD cb_add;
    CALLBACK_POOL_REMOVE cb_remove;
    int release_time;
    int end_flag;
    void* param;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct pool_t* pool_initialize(int init_num, int extend_num, CALLBACK_POOL_ADD cb_add, CALLBACK_POOL_REMOVE cb_remove, long timeout_ms, int ext_release_time, const void* param);
APIEXPORT void pool_finalize(struct pool_t* p);
APIEXPORT int pool_count(struct pool_t* p);
APIEXPORT void* pool_get(struct pool_t* p, int wait_time);
APIEXPORT void pool_release(struct pool_t* p, void* data);
APIEXPORT void pool_reset(struct pool_t* p, void* data);

#ifdef __cplusplus
}
#endif

#endif /* _POOL_H_ */
