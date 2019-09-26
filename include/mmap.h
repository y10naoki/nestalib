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
#ifndef _MMAP_H_
#define _MMAP_H_

#ifdef _WIN32
#include <windows.h>
#endif
#include "apiexp.h"

#define MMAP_READONLY   0
#define MMAP_READWRITE  1

#define MMAP_AUTO_SIZE  0

struct mmap_t {
    int open_mode;      /* open mode */
    int fd;             /* fileno */
    int64 size;         /* map size */
    int64 real_size;    /* real file size */
    void* ptr;          /* pointer in view */
    int64 offset;       /* current position in view */
    int64 view_offset;  /* view offset */
    int64 view_size;    /* map view size, zero is same map size */
    size_t pgsize;      /* page size for view offset */
#ifdef _WIN32
    HANDLE hFile;
    HANDLE hMap;
#endif
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct mmap_t* mmap_open(int fd, int mode, int64 view_size);
APIEXPORT void mmap_close(struct mmap_t* map);
APIEXPORT char* mmap_map(struct mmap_t* map, int64 size);
APIEXPORT char* mmap_mapping(struct mmap_t* map, int64 offset, int64 size);
APIEXPORT int64 mmap_seek(struct mmap_t* map, int64 offset);
APIEXPORT size_t mmap_read(struct mmap_t* map, void* data, size_t size);
APIEXPORT size_t mmap_write(struct mmap_t* map, const void* data, size_t size);
APIEXPORT int mmap_resize(struct mmap_t* map, int64 size);

#ifdef __cplusplus
}
#endif

#endif /* _MMAP_H_ */
