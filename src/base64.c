/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2008-2019 YAMAMOTO Naoki
 *
 * Original code: http://www.sea-bird.org/doc/Cygwin/BASE64enc.html
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
#include <memory.h>

#define API_INTERNAL
#include "apiexp.h"

static char* base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

typedef union {
    unsigned int  data;
    unsigned char xyz[4];
} BASE64_UNION_DATA;

static long codetovalue(unsigned char c)
{
    if ((c >= (unsigned char)'A') && (c <= (unsigned char)'Z'))
        return (long)(c - (unsigned char)'A');
    else if ((c >= (unsigned char)'a') && (c <= (unsigned char)'z'))
        return ((long)(c - (unsigned char)'a') + 26);
    else if ((c >= (unsigned char)'0') && (c <= (unsigned char)'9'))
        return ((long)(c - (unsigned char)'0') + 52);
    else if ((unsigned char)'+' == c)
        return (long)62;
    else if ((unsigned char)'/' == c)
        return (long)63;
    else if ((unsigned char)'=' == c)
        return 0;
    else
        return -1;
}

static int decode_str(int enc_ptr, const char* src, char* dest)
{
    int i, j;
    unsigned long base64 = 0;
    unsigned char x;
    BASE64_UNION_DATA bb;
    int bits = 0;
    int nb;

    for (i = enc_ptr; i < enc_ptr + 4; i++) {
        x = (unsigned char)codetovalue((unsigned char)src[i]);
        base64 |= x;
        if ((i - enc_ptr) != 3)
            base64 <<= 6;
        if (src[i] != '=')
            bits += 6;
    }

    base64 <<= 8;
    bb.data = (unsigned int)base64;

    for (j = 0, i = 3; i >= 1; i--)
        dest[j++] = bb.xyz[i];

    nb = bits / 8;
    return nb;
}

static void encode_char(unsigned long bb, int srclen, char* dest, int j)
{
    int x, i, base;

    /* 最終位置の計算 */
    for (i = srclen; i < 2; i++)
        bb <<= 8;

    /* BASE64変換 */
    for (base = 18, x = 0; x < srclen + 2; x++, base -= 6)
        dest[j++] = base64[(unsigned long)((bb>>base) & 0x3F)];

    /* 端数の判断 */
    for (i = x; i < 4; i++)
        dest[j++] = '=';     /* 端数 */
}

/*
 * データを base64 でエンコードします。
 * dst領域は src の約1.5倍の大きさが必要になります。
 * dst領域の終端に '\0' が付加されます。
 *
 * dst: base64エンコードされた文字列
 * src: 変換するデータ
 * src_size: 変換するデータのサイズ
 *
 * 戻り値
 *  dstのポインタを返します。
 */
APIEXPORT char* base64_encode(char* dst, const void* src, int src_size)
{
    unsigned char* p;
    unsigned long bb;
    int i, j;
    int n;

    p = (unsigned char*)src;
    bb = 0L;
    i = j = 0;
    n = 0;
    while (n < src_size) {
        bb <<= 8;
        bb |= (unsigned long)*p;

        /* 24bit単位に編集 */
        if (i == 2) {
            encode_char(bb, i, dst, j);

            j += 4;
            i = 0;
            bb = 0;
        } else {
            i++;
        }
        p++;
        n++;
    }

    /* 24bitに満たない残り */
    if (i > 0) {
        encode_char(bb, i - 1, dst, j);
        j += 4;
    }

    dst[j] = '\0';
    return dst;
}

/*
 * base64 でエンコードされた文字列をデコードします。
 *
 * dst: デコードされたデータ領域のポインタ
 * src: base64エンコードされたデータ
 *
 * 戻り値
 *  デコードされたデータサイズを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int base64_decode(void* dst, const char* src)
{
    int i = 0;
    size_t srclen;
    char* dp;
    char tmp[3+1];
    int n = 0;

    if (src == NULL)
        return -1;

    srclen = strlen(src);
    if ((srclen % 4) > 0)
        return -1;

    dp = (char*)dst;
    while (srclen > 0) {
        int m;

        m = decode_str(i, src, tmp);
        memcpy(dp, tmp, m);

        i += 4;
        srclen -= 4;
        n += m;
        dp += m;
    }
    return n;
}
