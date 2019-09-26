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

static int find_qparam(struct request_t* req, const char* name)
{
    int i;

    for (i = 0; i < req->q_param.count; i++) {
        if (stricmp(req->q_param.vt[i].name, name) == 0)
            return i;
    }
    return -1;    /* notfound */
}

/*
 * リクエストパラメータから指定されたパラメータ名を検索して値を返します。
 * パラメータ名は大文字と小文字を区別しません。
 *
 * 戻り値
 *  値のポインタを返します。空値の場合は "\0" の文字列を返します。
 *  パラメータ名が存在しない場合は NULL を返します。
 */
APIEXPORT char* get_qparam(struct request_t* req, const char* name)
{
    int index;

    index = find_qparam(req, name);
    if (index < 0)
        return NULL;
    return req->q_param.vt[index].value;
}

/*
 * リクエストパラメータ数を返します。
 */
APIEXPORT int get_qparam_count(struct request_t* req)
{
    return req->q_param.count;
}


/*
 * 添付ファイル構造体のポインタを取得します。
 * リクエストパラメータから指定された添付ファイルのパラメータ名を検索します。
 * パラメータ名は大文字と小文字を区別しません。
 *
 * 戻り値
 *  添付ファイル構造体のポインタを返します。
 *  存在しない場合は NULL を返します。
 */
APIEXPORT struct attach_file_t* get_attach_file(struct request_t* req, const char* name)
{
    int index;

    index = find_qparam(req, name);
    if (index < 0)
        return NULL;
    return req->q_param.af[index];
}
