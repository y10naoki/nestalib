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

/*
 * リクエスト内のヒープメモリを確保します。
 * ここで確保されたメモリはレスポンスを返した時点で自動的に解放されます。
 *
 * req: リクエスト構造体のポインタ
 * size: 確保するバイト数
 *
 * 戻り値
 *  確保された領域のポインタを返します。
 *  エラーの場合は NULL が返されます。
 */
APIEXPORT void* xalloc(struct request_t* req, int size)
{
    void* ptr;

    ptr = malloc(size);
    if (ptr == NULL) {
        err_log(req->addr, "ralloc(): no memory(%d bytes).", size);
        return NULL;
    }

    if (vect_append(req->heap, ptr) < 0) {
        free(ptr);
        return NULL;
    }
    return ptr;
}

/*
 * リクエスト内のヒープメモリのサイズを拡張します。
 * ここで確保されたメモリはレスポンスを返した時点で自動的に解放されます。
 *
 * req: リクエスト構造体のポインタ
 * resize: 確保するバイト数
 *
 * 戻り値
 *  確保された領域のポインタを返します。
 *  エラーの場合は NULL が返されます。
 */
APIEXPORT void* xrealloc(struct request_t* req, const void* ptr, int resize)
{
    void* new_ptr;

    new_ptr = realloc((void*)ptr, resize);
    if (new_ptr == NULL) {
        err_log(req->addr, "rrealloc(): no memory(%d bytes).", resize);
        return NULL;
    }

    if (ptr != new_ptr)
        vect_update(req->heap, ptr, new_ptr);

    return new_ptr;
}

/*
 * リクエスト内のヒープメモリを解放します。
 *
 * req: リクエスト構造体のポインタ
 * ptr: 解放するポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void xfree(struct request_t* req, const void* ptr)
{
    vect_delete(req->heap, ptr);
    free((void*)ptr);
}
