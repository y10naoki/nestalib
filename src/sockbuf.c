/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2011-2019 YAMAMOTO Naoki
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

#define SOCK_BUFSIZE    4096

static int sockbuf_recv(struct sock_buf_t* sb)
{
    int recv_len;

    SAFE_SYSCALL(recv_len, (int)recv(sb->socket, sb->buf, sb->bufsize, 0));
    if (recv_len < 0)
        return -1;
    if (recv_len == 0)
        return 0;
    sb->cur_size = recv_len;
    return recv_len;
}

static int sockbuf_pushback(struct sock_buf_t* sb, const char* buf, int size)
{
    int nsize;
    char* dst;

    nsize = sb->cur_size + size;
    if (nsize > sb->bufsize) {
        char* tp;

        tp = (char*)realloc(sb->buf, nsize);
        if (tp == NULL) {
            err_write("sockbuf_pushback: buffer over flow, no memory size=%d", nsize);
            return -1;
        }
        sb->buf = tp;
        sb->bufsize = nsize;
    }
    dst = sb->buf + size;
    if (sb->cur_size > 0)
        memmove(sb->buf, dst, sb->cur_size);
    memcpy(sb->buf, buf, size);
    sb->cur_size = nsize;
    return 0;
}

/*
 * ソケットバッファを確保します。
 *
 * socket: ソケットハンドル
 *
 * 戻り値
 *  ソケットバッファ構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct sock_buf_t* sockbuf_alloc(SOCKET socket)
{
    struct sock_buf_t* sb;

    sb = (struct sock_buf_t*)malloc(sizeof(struct sock_buf_t));
    if (sb == NULL) {
        err_write("sockbuf_open: no memory");
        return NULL;
    }

    sb->socket = socket;
    sb->bufsize = SOCK_BUFSIZE;
    sb->cur_size = 0;

    sb->buf = (char*)malloc(sb->bufsize);
    if (sb->buf == NULL) {
        err_write("sockbuf_open: no memory");
        free(sb);
        return NULL;
    }
    return sb;
}

/*
 * ソケットバッファを開放します。
 * ソケットのクローズは行われません。
 *
 * sb: ソケットバッファ構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void sockbuf_free(struct sock_buf_t* sb)
{
    if (sb) {
        if (sb->buf)
            free(sb->buf);
        free(sb);
    }
}

/*
 * 受信データがあるか調べます。
 *
 * sb: ソケットバッファ構造体のポインタ
 * timeout_ms: 受信データがあるかチェックする最大待ち時間（ミリ秒）
 *
 * 戻り値
 *  受信データがある場合は 1 を返します。
 *  受信データがない場合はゼロを返します。
 */
APIEXPORT int sockbuf_wait_data(struct sock_buf_t* sb, int timeout_ms)
{
    if (sb->cur_size > 0)
        return 1;
    return wait_recv_data(sb->socket, timeout_ms);
}

/*
 * ソケットバッファから最大バイト数分のchar型データを受信します。
 * 確実に size分を受信する場合は sockbuf_nchar() を使用します。
 *
 * sb: ソケットバッファ構造体のポインタ
 * buf: 受信したデータが設定される領域のアドレス
 * size: 受信するデータ領域のバイト数
 *
 * 戻り値
 *  正常に受信できた場合は受信したバイト数を返します。
 *  FINを受信した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sockbuf_read(struct sock_buf_t* sb, char* buf, int size)
{
    int len;

    if (sb->cur_size < 1) {
        /* バッファを充填します。*/
        len = sockbuf_recv(sb);
        if (len < 1)
            return len;
    }

    if (size <= sb->cur_size) {
        memcpy(buf, sb->buf, size);
        len = size;
        sb->cur_size -= size;
        if (sb->cur_size > 0)
            memmove(sb->buf, &sb->buf[size], sb->cur_size);
    } else {
        memcpy(buf, sb->buf, sb->cur_size);
        len = sb->cur_size;
        sb->cur_size = 0;
    }
    return len;
}

/*
 * ソケットバッファからバイト数分のchar型データを受信します。
 *
 * sb: ソケットバッファ構造体のポインタ
 * buf: 受信したデータが設定される領域のアドレス
 * size: 受信するデータ領域のバイト数
 *
 * 戻り値
 *  正常に受信できた場合は受信したバイト数を返します。
 *  FINを受信した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sockbuf_nchar(struct sock_buf_t* sb, char* buf, int size)
{
    int recv_bytes = 0;

    do {
        int rlen;
        int rbytes;

        rbytes = size - recv_bytes;
        rlen = sockbuf_read(sb, &buf[recv_bytes], rbytes);
        if (rlen < 1)
            return rlen;
        recv_bytes += rlen;
    } while (recv_bytes < size);
    return size;
}

/*
 * ソケットバッファから short型のデータを受信します。
 * short型は符号付2バイト整数です。
 * バイト順はプラットフォームに依存します。
 *
 * sb: ソケットバッファ構造体のポインタ
 * status: 状態を示すアドレス
 *           正常に受信できた場合はゼロが設定されます。
 *           FINを受信した場合は 1 が設定されます。
 *           エラーの場合は -1 が設定されます。
 *
 * 戻り値
 *  受信したshortデータを返します。
 */
APIEXPORT short sockbuf_short(struct sock_buf_t* sb, int* status)
{
    short data;
    int len;

    *status = 0;
    len = sockbuf_read(sb, (char*)&data, sizeof(short));
    if (len < 0) {
        *status = -1;
        return 0;
    }
    if (len == 0) {
        *status = 1;  /* FIN受信 */
        return 0;
    }
    return data;
}

/*
 * ソケットから int型のデータを受信します。
 * int型は符号付4バイト整数です。
 * バイト順はプラットフォームに依存します。
 *
 * sb: ソケットバッファ構造体のポインタ
 * status: 状態を示すアドレス
 *           正常に受信できた場合はゼロが設定されます。
 *           FINを受信した場合は 1 が設定されます。
 *           エラーの場合は -1 が設定されます。
 *
 * 戻り値
 *  受信したintデータを返します。
 */
APIEXPORT int sockbuf_int(struct sock_buf_t* sb, int* status)
{
    int data;
    int len;

    *status = 0;
    len = sockbuf_read(sb, (char*)&data, sizeof(int));
    if (len < 0) {
        *status = -1;
        return 0;
    }
    if (len == 0) {
        *status = 1;  /* FIN受信 */
        return 0;
    }
    return data;
}

/*
 * ソケットから int64型のデータを受信します。
 * int64型は符号付8バイト整数です。
 * バイト順はプラットフォームに依存します。
 *
 * sb: ソケットバッファ構造体のポインタ
 * status: 状態を示すアドレス
 *           正常に受信できた場合はゼロが設定されます。
 *           FINを受信した場合は 1 が設定されます。
 *           エラーの場合は -1 が設定されます。
 *
 * 戻り値
 *  受信したint64データを返します。
 */
APIEXPORT int64 sockbuf_int64(struct sock_buf_t* sb, int* status)
{
    int64 data;
    int len;

    *status = 0;
    len = sockbuf_read(sb, (char*)&data, sizeof(int64));
    if (len < 0) {
        *status = -1;
        return 0;
    }
    if (len == 0) {
        *status = 1;  /* FIN受信 */
        return 0;
    }
    return data;
}

/*
 * ソケットバッファから区切り文字列までのchar型データを受信します。
 * 区切り文字列が見つからない場合は size-1 バイト分のデータを受信します。
 * データの最後に '\0' が付加されます。
 *
 * 受信データに区切り文字列を含めるかどうかは delim_add_flag によります。
 * 真の場合は区切り文字がバッファに設定されます。
 * 偽の場合は区切り文字はバッファに含まれません。
 *
 * 区切り文字を受信した場合は found_flag に 1 が設定されます。
 * バッファサイズ分(size-1)受信した場合は found_flag が 0 に設定されます。
 *
 * sb: ソケットバッファ構造体のポインタ
 * buf: 受信したデータが設定される領域のアドレス
 * size: 受信するデータ領域のバイト数
 * delim: 区切り文字列
 * delim_add_flag: 区切り文字列をバッファの末尾に付加するフラグ
 * found_flag: 区切り文字列を受信したかどうかが設定される領域のアドレス
 *
 * 戻り値
 *  正常に受信できた場合は受信したバイト数を返します。
 *  FINを受信した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sockbuf_gets(struct sock_buf_t* sb,
                           char* buf,
                           int size,
                           const char* delim,
                           int delim_add_flag,
                           int* found_flag)
{
    int recv_size = 0;
    int rest_size;
    int delim_len;

    rest_size = size - 1;
    delim_len = (int)strlen(delim);
    *found_flag = 0;

    while (rest_size > 0) {
        int len;
        int index;

        len = sockbuf_read(sb, &buf[recv_size], rest_size);
        if (len < 1) {
            if (recv_size > 0)
                return recv_size;
            return len;
        }
        recv_size += len;
        buf[recv_size] = '\0';
        rest_size -= len;

        index = indexofstr(buf, delim);
        if (index >= 0) {
            int next;
            int back_size;

            /* 受取り過ぎたデータがあればバッファへ戻します。*/
            next = index + delim_len;
            back_size = recv_size - next;
            if (back_size > 0) {
                if (sockbuf_pushback(sb, &buf[next], back_size) < 0)
                    return -1;
            }
            if (delim_add_flag)
                buf[next] = '\0';
            else
                buf[index] = '\0';
            *found_flag = 1;
            break;
        }
    }
    return (int)strlen(buf);
}
