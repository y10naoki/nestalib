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
 * レスポンスの初期処理を行ないます。
 * レスポンス構造体のメモリを確保します。
 *
 * socket: ソケット
 *
 * 戻り値
 *  レスポンス構造体のポインタ
 */
APIEXPORT struct response_t* resp_initialize(SOCKET socket)
{
    struct response_t *resp;

    resp = (struct response_t*)malloc(sizeof(struct response_t));
    if (resp == NULL) {
        err_write("rsponse: No memory.");
        return NULL;
    }

    resp->socket = socket;
    resp->content_size = 0;
    return resp;
}

/*
 * レスポンスを終了します。
 * 確保された領域は解放されます。
 *
 * resp: レスポンス構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void resp_finalize(struct response_t* resp)
{
    free(resp);
}

/*
 * HTTPヘッダーをレスポンスします。
 *
 * resp: レスポンス構造体のポインタ
 * hdr: 送信するヘッダー構造体のポインタ
 *
 * 戻り値
 *  送信したバイト数を返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int resp_send_header(struct response_t* resp, struct http_header_t* hdr)
{
    return send_header(resp->socket, hdr);
}

/*
 * HTTPボディをレスポンスします。
 * レスポンス構造体のコンテントサイズが設定されます。
 *
 * resp: レスポンス構造体のポインタ
 * body: 送信するデータのポインタ
 * body_size: 送信するデータサイズ
 *
 * 戻り値
 *  送信したバイト数を返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int resp_send_body(struct response_t* resp, const void* body, int body_size)
{
    int result;

    result = send_data(resp->socket, body, body_size);
    if (result < 0)
        return -1;
    resp->content_size = body_size;
    return result;
}

/*
 * HTTPデータをレスポンスします。
 *
 * resp: レスポンス構造体のポインタ
 * body: 送信するデータのポインタ
 * body_size: 送信するデータサイズ
 *
 * 戻り値
 *  送信したバイト数を返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int resp_send_data(struct response_t* resp, const void* data, int data_size)
{
    int result;

    result = send_data(resp->socket, data, data_size);
    if (result < 0)
        return -1;
    return result;
}

/*
 * コンテントサイズを設定します。
 *
 * resp: レスポンス構造体のポインタ
 * content_size: コンテントサイズ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void resp_set_content_size(struct response_t* resp, int content_size)
{
    resp->content_size = content_size;
}
