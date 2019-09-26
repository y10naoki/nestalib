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
#ifndef _DATAIO_H_
#define _DATAIO_H_

#include "apiexp.h"
#include "nestalib.h"

#define DIO_FREE_COUNT 20   /* (256 - 16) / 12 */

struct dio_free_t {
    long offset;
    int count;
    int pagesize[DIO_FREE_COUNT];
    long dataptr[DIO_FREE_COUNT];
    long nextptr;
};

struct dio_data_t {
    CS_DEF(critical_section);
    int fd;
    long freeptr;
    struct dio_free_t* freepage;
    struct mmap_t* mmap;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* dataio.c */
APIEXPORT struct dio_data_t* dio_open(const char* fname);
APIEXPORT int dio_create(const char* fname);
APIEXPORT void dio_close(struct dio_data_t* bio);
APIEXPORT long dio_avail_space(struct dio_data_t* bio, int size);
APIEXPORT int dio_read(struct dio_data_t* bio, long ptr, void* data, int size);
APIEXPORT int dio_write(struct dio_data_t* bio, long ptr, const void* data, int size);
APIEXPORT int dio_delete(struct dio_data_t* bio, long ptr);
int dio_data_size(struct dio_data_t* bio, long ptr);
int dio_area_size(struct dio_data_t* dio, long ptr);

#ifdef __cplusplus
}
#endif

#endif /* _DATAIO_H_ */
