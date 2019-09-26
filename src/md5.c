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

#include <string.h>
#include <stdio.h>
#define API_INTERNAL
#include "md5.h"

/*
 * MD5 のメッセージダイジェストを求めます。
 * 32バイトの16進表記文字を返します。
 *
 * http://www.ipa.go.jp/security/rfc/RFC1321JA.html
 *
 * dst: ダイジェスト結果を格納する領域(32+1バイト)
 * str: 変換する文字列
 *
 * 戻り値
 *  dstのポインタを返します。
 */
APIEXPORT char* md5(char* dst, const char* str)
{
    RSAMD5_CTX ctx;
    unsigned char digest[16];
    char* p;
    int i;

    RSAMD5Init(&ctx);
    RSAMD5Update(&ctx, (unsigned char*)str, (unsigned int)strlen(str));
    RSAMD5Final(digest, &ctx);

    p = dst;
    for (i = 0; i < 16; i++) {
        sprintf(p, "%02x", digest[i]);
        p += strlen(p);
    }
    return dst;
}
