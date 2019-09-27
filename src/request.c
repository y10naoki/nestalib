/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2008-2019 YAMAMOTO Naoki
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

#define INIT_HEAP_SIZE 10
#define MAX_MULTIPART_LINE_SIZE 2048

static int split_query(char* str, struct variable_t* vt, char delim)
{
    if (split_item(str, vt, delim) < 0)     /* header.c */
        return -1;
    unescape_url(vt->value);
    return 0;
}

/*
 * クエリ変数と値を構造体(struct request_t)に設定します。
 *
 * 戻り値
 *  設定した個数を返します。
 *  エラーの場合は -1 を返します。
 */
static int set_query_param(struct request_t* req, char* qs)
{
    int i;
    char tbuf[MAX_VNAME_SIZE+MAX_VVALUE_SIZE+2];

    for (i = 0; i < MAX_REQ_VARIABLE && qs[0] != '\0'; i++) {
        int index;

        /* パラメーターは'&'で区切られる */
        index = indexof(qs, '&');
        if (index < 0)
            index = (int)strlen(qs);
        if (index > sizeof(tbuf)-2)
            return -1;  /* too long */

        splitword(tbuf, qs, '&');
        if (split_query(tbuf, &req->q_param.vt[i], '=') < 0)
            return -1;
    }
    req->q_param.count = i;
    return req->q_param.count;
}

static int set_multipart_query_param(struct request_t* req,
                                     const char* name,
                                     const char* value)
{
    int index;
    struct variable_t* vt;

    index = req->q_param.count;
    vt = &req->q_param.vt[index];
    vt->name = (char*)malloc(strlen(name) + strlen(value) + 2);
    if (vt->name == NULL) {
        err_write("set_multipart_param: no memory");
        return -1;
    }
    
    strcpy(vt->name, name);
    vt->value = vt->name + strlen(name) + 1;
    strcpy(vt->value, value);    
    unescape_url(vt->value);

    req->q_param.count++;
    return req->q_param.count;
}

static int set_multipart_query_binary(struct request_t* req,
                                      const char* name,
                                      const void* data,
                                      int datasize,
                                      char** value_ptr)
{
    int index;
    struct variable_t* vt;
    
    index = req->q_param.count;
    vt = &req->q_param.vt[index];
    vt->name = (char*)malloc(strlen(name) + datasize + 2);
    if (vt->name == NULL) {
        err_write("set_multipart_query_binary: no memory");
        return -1;
    }
    
    strcpy(vt->name, name);
    vt->value = vt->name + strlen(name) + 1;
    memcpy(vt->value, data, datasize);
    *value_ptr = vt->value;

    req->q_param.count++;
    return req->q_param.count;
}

static int set_attach_info(struct request_t* req,
                           int index,
                           const char* filename,
                           int data_size,
                           const char* data,
                           const char* mime_type,
                           const char* charset)
{
    struct attach_file_t* af;

    af = (struct attach_file_t*)malloc(sizeof(struct attach_file_t));
    if (af == NULL) {
        err_write("set_attach_info: no memory");
        return -1;
    }

    strncpy(af->filename, filename, sizeof(af->filename)-1);
    strncpy(af->mimetype, mime_type, sizeof(af->mimetype)-1);
    strncpy(af->charset, charset, sizeof(af->charset)-1);
    af->size = data_size;
    af->data = (unsigned char*)data;

    req->q_param.af[index] = af;
    return 0;
}

/* 
 * content_type から boundary= を探します。
 * Content-Type: multipart/form-data; boundary=-AabbZ
 */
static char* get_boundary_str(const char* content_type, char* boundary)
{
    char* bp;
    int index;

    bp = strstr(content_type, "boundary=");
    if (bp == NULL)
        return NULL;
    bp += (sizeof("boundary=") - 1);
    strcpy(boundary, bp);
    index = indexof(boundary, ';');
    if (index > 0)
        boundary[index] = '\0';
    return trim(boundary);
}

/* bp から CRLF の直前までの内容を linebuf に設定します。
 * 行末のCRLFは設定されません。
 * linebufの終端は '\0' が設定されます。
 *
 * CRLF の次の位置を返します。
 */ 
static char* get_boundary_line(char* bp, char* linebuf)
{
    int index;

    index = indexofstr(bp, "\r\n");
    if (index < 0)
        return NULL;
    if (index > MAX_MULTIPART_LINE_SIZE)
        return NULL;
    memcpy(linebuf, bp, index);
    linebuf[index] = '\0';
    bp += index + (sizeof("\r\n") - 1);
    return bp;
}

static int parse_content_disposition(char* buf, char* name, char* filename)
{
    char** listp;
    char* p;
    int index;

    listp = split(buf, ';');
    if (listp == NULL)
        return -1;

    *name = '\0';
    *filename = '\0';

    if (listp[0] != NULL) {
        trim(listp[0]);
        if (strcmp(listp[0], "form-data") != 0) {
            list_free(listp);
            return -1;
        }
    }
    if (listp[1] != NULL) {
        trim(listp[1]);
        if (memcmp(listp[1], "name=", sizeof("name=")-1) == 0) {
            p = strstr(listp[1], "name=");
            if (p != NULL) {
                p += (sizeof("name=") - 1);
                p++;  /* ダブルクォートをスキップ */
                index = indexof(p, '\"');
                if (index >= 0) {
                    if (index < MAX_VNAME_SIZE)
                        substr(name, p, 0, index);
                }
            }
        }
    }
    if (listp[2] != NULL) {
        trim(listp[2]);
        if (memcmp(listp[2], "filename=", sizeof("filename=")-1) == 0) {
            p = strstr(listp[2], "filename=");
            if (p != NULL) {
                p += (sizeof("filename=") - 1);
                p++;  /* ダブルクォートをスキップ */
                index = indexof(p, '\"');
                if (index >= 0) {
                    if (index < MAX_PATH-1)
                        substr(filename, p, 0, index);
                }
            }
        }
    }
    list_free(listp);
    return 0;
}

static int parse_content_type(char* buf, char* mime_type, char* charset)
{
    char** listp;
    char* p;
    int index;

    listp = split(buf, ';');
    if (listp == NULL)
        return -1;

    *mime_type = '\0';
    *charset = '\0';

    if (listp[0] != NULL)
        strcpy(mime_type, trim(listp[0]));

    if (listp[1] != NULL) {
        trim(listp[1]);
        if (memcmp(listp[1], "charset=", sizeof("charset=")-1) == 0) {
            p = strstr(listp[1], "charset=");
            if (p != NULL) {
                p += (sizeof("charset=") - 1);
                p++;  /* ダブルクォートをスキップ */
                index = indexof(p, '\"');
                if (index >= 0) {
                    if (index < MAX_VNAME_SIZE)
                        substr(charset, p, 0, index);
                }
            }
        }
    }
    list_free(listp);
    return 0;
}

static int search_boundary_binary(const char* p, const char* boundary, const char* eop)
{
    int n = 0;
    int cr_flag = 0;

    if (p == NULL)
        return 0;

    for (; p < eop; p++) {
        if (*p == '\r') {
            cr_flag = 1;
        } else if (*p == '\n') {
            if (cr_flag) {
                if (memcmp(p+1, boundary, strlen(boundary)) == 0)
                    break;
            }
            cr_flag = 0;
        } else
            cr_flag = 0;
        n++;
    }
    if (p == eop)
        return -1;
    return n - 1;
}

static char* get_multipart_value(char* bp,
                                 const char* boundary,
                                 char* linebuf,
                                 int linebuf_size)
{
    char eob[MAX_MULTIPART_LINE_SIZE+3];
    int index;

    snprintf(eob, sizeof(eob), "\r\n%s", boundary);
    index = indexofstr(bp, eob);
    if (index < 0)
        return NULL;
    if (index > linebuf_size-1)
        return NULL;
    memcpy(linebuf, bp, index);
    linebuf[index] = '\0';
    bp += index + (sizeof("\r\n") - 1);
    return bp;
}

static char* get_attach_data(char* bp,
                             const char* boundary,
                             const char* mime_type,
                             const char* eop,
                             char** data_ptr, int* size)
{
    int pad_null = 0;
    int dsize = 0;

    if (strnicmp(mime_type, "text", sizeof("text")-1) == 0) {
        char eob[MAX_MULTIPART_LINE_SIZE+3];
        int index;

        snprintf(eob, sizeof(eob), "\r\n%s", boundary);
        index = indexofstr(bp, eob);
        if (index < 0)
            return NULL;
        dsize = index;
        pad_null = 1;
    } else {
        /* バイナリデータを取得します。*/
        dsize = search_boundary_binary(bp, boundary, eop);
        if (dsize < 0)
            return NULL;
    }
    *data_ptr = (char*)malloc(dsize + pad_null);
    if (*data_ptr == NULL) {
        err_write("get_attach_data(): no memory, size=%d", dsize);
        return NULL;
    }
    memcpy(*data_ptr, bp, dsize);
    if (pad_null)
        (*data_ptr)[dsize] = '\0';
    bp += dsize + (sizeof("\r\n") - 1);
    *size = dsize;
    return bp;
}

static int set_multipart_query(struct request_t* req,
                               const char* body_ptr,
                               int content_length,
                               const char* hdr_content_type)
{
    char* boundary;
    char* bp;
    char* stmp;
    char* edmp;

    /* バウンダリ名を取得します。*/
    boundary = (char*)alloca(strlen(hdr_content_type) + 1);
    if (get_boundary_str(hdr_content_type, boundary) == NULL)
        return -1;
    
    bp = (char*)body_ptr;
    stmp = (char*)alloca(strlen(boundary)+3);
    edmp = (char*)alloca(strlen(boundary)+5);
    sprintf(stmp, "--%s", boundary);  /* multipartの開始 */
    sprintf(edmp, "--%s--", boundary);  /* multipartの終了 */

    while (bp) {
        char linebuf[MAX_MULTIPART_LINE_SIZE+1];
        char name[MAX_VNAME_SIZE+1];
        char filename[MAX_PATH];
        char value[MAX_VVALUE_SIZE+1];
        char mime_type[MAX_VNAME_SIZE+1];
        char charset[MAX_VNAME_SIZE+1];

        bp = get_boundary_line(bp, linebuf);
        if (strcmp(linebuf, stmp) == 0) {
            /* バウンダリ毎の処理を開始します。*/
            name[0] = '\0';
            filename[0] = '\0';
            value[0] = '\0';
            mime_type[0] = '\0';
            charset[0] = '\0';
        } else if (memcmp(linebuf, "Content-Disposition:", sizeof("Content-Disposition:")-1) == 0) {
            char* p = &linebuf[sizeof("Content-Disposition:")-1];
            if (parse_content_disposition(p, name, filename) < 0)
                return -1;
        } else if (memcmp(linebuf, "Content-Type:", sizeof("Content-Type:")-1) == 0) {
            char* p = &linebuf[sizeof("Content-Type:")-1];
            if (parse_content_type(p, mime_type, charset) < 0)
                return -1;
        } else if (linebuf[0] == '\0') {
            if (filename[0] == '\0') {
                /* 次のバウンダリの開始まで linebuf に読み込みます。*/
                bp = get_multipart_value(bp, stmp, linebuf, sizeof(linebuf));
                if (bp == NULL)
                    return -1;
                if (strlen(linebuf) > MAX_VVALUE_SIZE)
                    return -1;
                strcpy(value, linebuf);
                /* name/value をクエリパラメータに設定します。*/
                if (set_multipart_query_param(req, name, value) < 0)
                    return -1;
            } else {
                char* eop;
                char* attach_ptr;
                int attach_size;

                /* 添付ファイルのデータを取得します。*/
                eop = (char*)body_ptr + content_length;
                bp = get_attach_data(bp, stmp, mime_type, eop, &attach_ptr, &attach_size);
                if (bp == NULL)
                    return -1;
                /* データをクエリパラメータに設定します。*/
                if (attach_ptr != NULL) {
                    int q_count;
                    char* v_ptr;

                    q_count = set_multipart_query_binary(req, name, attach_ptr, attach_size, &v_ptr);
                    if (q_count < 0)
                        return -1;
                    /* ファイル情報をクエリパラメータに設定します。*/
                    if (set_attach_info(req, q_count-1, filename, attach_size, v_ptr, mime_type, charset) < 0)
                        return -1;
                    free(attach_ptr);
                }
            }
        } else if (strcmp(linebuf, edmp) == 0) {
            /* バウンダリの終わり */
            break;
        }
    }
    return 0;
}

/*
 * 受信データをリクエスト構造体に設定してそのポインタを返します。
 * statusの領域には HTTP_STATUS が設定されます。
 *
 * 戻り値
 *  リクエスト構造体のポインタ
 *  この領域は関数内で動的に確保された領域なので使用後にreq_free()関数で解放する必要があります。
 *  エラーの場合は NULL が返されます。
 */
APIEXPORT struct request_t* get_request(SOCKET socket, struct in_addr addr, int* status)
{
    struct request_t* req;
    char* req_ptr;
    char* header_ptr;
    char* body_ptr;
    char* tp;
    char method[MAX_METHOD_LINE_SIZE],
         uri[MAX_METHOD_LINE_SIZE],
         protocol[MAX_METHOD_LINE_SIZE];
    int len;

    /* リクエスト文字列をすべて req_ptr に受信します。*/
    req_ptr = recv_data(socket, MAX_RECV_DATA_SIZE, 1000, NULL, NULL);
    if (req_ptr == NULL) {
        *status = HTTP_INTERNAL_SERVER_ERROR;
        return NULL;
    }

    /* １行目を取り出します。*/
    tp = strstr(req_ptr, "\r\n");
    if (tp == NULL) {
        recv_free(req_ptr);
        *status = HTTP_BADREQUEST;
        return NULL;
    }
    *tp = '\0';

    len = (int)strlen(req_ptr);
    if (len > MAX_METHOD_LINE_SIZE-1) {
        err_log(addr, "get_request: length(%d) too large.", len);
        recv_free(req_ptr);
        *status = HTTP_REQUEST_URI_TOO_LONG;
        return NULL;
    }
    /* 先頭行の method url protocolを取得 */
    if (sscanf(req_ptr, "%s %s %s", method, uri, protocol) != 3) {
        err_log(addr, "get_request: Bad request: %s", req_ptr);
        recv_free(req_ptr);
        *status = HTTP_BADREQUEST;
        return NULL;
    }
    /* protocol チェック */
    if (strcmp(protocol, "HTTP/1.0") && strcmp(protocol, "HTTP/1.1")) {
        err_log(addr, "get_request: Bad request protocol: %s", protocol);
        recv_free(req_ptr);
        *status = HTTP_BADREQUEST;
        return NULL;
    }
    /* method チェック */
    if (strcmp(method, "GET") && strcmp(method, "POST") && strcmp(method, "HEAD")) {
        err_log(addr, "get_request: Bad request method: %s", method);
        recv_free(req_ptr);
        *status = HTTP_BADREQUEST;
        return NULL;
    }
    /* uri チェック */
    if (strlen(uri) > MAX_URI_LENGTH) {
        err_log(addr, "get_request: URI length too large: %s", uri);
        recv_free(req_ptr);
        *status = HTTP_REQUEST_URI_TOO_LONG;
        return NULL;
    }

    /* メモリの確保 */
    req = (struct request_t*)calloc(1, sizeof(struct request_t));
    if (req == NULL) {
        err_log(addr, "get_request: No memory.");
        recv_free(req_ptr);
        *status = HTTP_INTERNAL_SERVER_ERROR;
        return NULL;
    }
    strcpy(req->method, method);
    strcpy(req->uri, uri);
    strcpy(req->protocol, protocol);

    /* 開始時刻(usec) */
    req->start_time = system_time();

    /* クライアントのIPアドレス */
    req->addr = addr;

    /* リクエスト用ヒープメモリ管理領域を確保 */
    req->heap = vect_initialize(INIT_HEAP_SIZE);

    /* クエリ文字列を取り出して変数に設定します。*/
    req->qs_index = indexof(req->uri, '?');
    if (req->qs_index > 0) {
        char qs[MAX_URI_LENGTH];

        /* クエリ文字列（変数）*/
        substr(qs, req->uri, req->qs_index+1, -1);
        if (set_query_param(req, qs) < 0) {
            err_log(addr, "get_request: Bad query string: %s", qs);
            recv_free(req_ptr);
            *status = HTTP_BADREQUEST;
            return req;
        }
        /* コンテント名（最初の / は外します）*/
        substr(req->content_name, req->uri, 1, req->qs_index-1);
    } else {
        /* コンテント名（最初の / は外します）*/
        strcpy(req->content_name, &req->uri[1]);
    }

    /* ヘッダー名と文字列に分解して設定します。*/
    header_ptr = req_ptr + len + sizeof("\r\n") - 1;
    body_ptr = split_header(header_ptr, &req->header);

    if (body_ptr != NULL) {
        if (strcmp(req->method, "POST") == 0 && *body_ptr) {
            int content_length = 0;
            char* hv;

            hv = get_http_header(&req->header, "Content-Length");
            if (hv != NULL)
                content_length = atoi(hv);
            if (content_length > 0) {
                /* POSTクエリパラメータの処理 */
                hv = get_http_header(&req->header, "Content-Type");
                if (strnicmp(hv, "multipart/form-data", sizeof("multipart/form-data")-1) == 0) {
                    /* multipart */
                    if (set_multipart_query(req, body_ptr, content_length, hv) < 0) {
                        err_log(addr, "get_request: Bad POST multipart query string.");
                        recv_free(req_ptr);
                        *status = HTTP_BADREQUEST;
                        return req;
                    }
                } else {
                    if (set_query_param(req, body_ptr) < 0) {
                        err_log(addr, "get_request: Bad POST query string.");
                        recv_free(req_ptr);
                        *status = HTTP_BADREQUEST;
                        return req;
                    }
                }
            }
        }
    }
    recv_free(req_ptr);

    *status = HTTP_OK;
    return req;
}

APIEXPORT void req_free(struct request_t* req)
{
    if (req != NULL) {
        int i;

        /* セッションを切り離します。*/
        if (req->session) {
            /* セッションリレーのセッションコピーが有効の場合は
               この関数内で行われます。*/
            ssn_detach(req->session);
        }

        /* リクエストヒープメモリの解放 */
        if (req->heap) {
            int n;

            n = vect_count(req->heap);
            for (i = 0; i < n; i++) {
                void* ptr;

                ptr = vect_get(req->heap, i);
                if (ptr != NULL)
                    free(ptr);
            }
            vect_finalize(req->heap);
        }

        /* ヘッダーの解放 */
        for (i = 0; i < req->header.count; i++)
            free_item(&req->header.vt[i]);

        /* クエリの解放 */
        for (i = 0; i < req->q_param.count; i++) {
            free_item(&req->q_param.vt[i]);
            /* 添付ファイル情報の解放 */
            if (req->q_param.af[i])
                free(req->q_param.af[i]);
        }

        /* リクエスト構造体の解放 */
        free(req);
    }
}
