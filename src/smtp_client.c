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

#include <stdlib.h>
#define API_INTERNAL
#include "nestalib.h"

/* 添付ファイル */
struct smtp_attach_file_t {
    const char* content_type;
    const char* transfer_encoding;
    const char* file_name;
    const char* enc_data;
};

/*
 * SMTP を利用してメール送信を行います。
 *
 * 日本語を使用する場合は呼び出し側で JISコードに変換する必要があります。
 * Message-ID: は設定されませんので呼び出し側で設定します。
 *
 * 添付ファイルは複数指定できます。
 * multipart/mixed で送信されます。
 */

/*
 * SMTPサーバーへコマンドを送信して応答コードをチェックします。
 * 応答コードのチェックだけを行う場合は send_buf を NULL にして使用します。
 * recv_buf が NULL の場合は、サーバーからの応答は行いません。
 */
static int smtp_protocol(struct smtp_session_t* smtp,
                         const char* send_buf,
                         char* recv_buf, int recv_buf_size,
                         const char* check_status)
{
    if (send_buf != NULL) {
        /* サーバーへデータを送信します。*/
        if (send_data(smtp->c_socket, send_buf, strlen(send_buf)) < 0) {
            err_write("smtp_protocol error: [%s] %s", smtp->server, strerror(errno));
            return -1;
        }
    }

    if (recv_buf != NULL) {
        int recv_size;

        /* サーバーから応答があるまで最大5秒待ちます。*/
        if (wait_recv_data(smtp->c_socket, 5000) < 1) {
            err_write("smtp_protocol timeout: [%s]", smtp->server);
            return -1;
        }
        /* サーバーから応答データを受信します。*/
        recv_size = recv(smtp->c_socket, recv_buf, recv_buf_size, 0);
        if (recv_size < 0)
            return -1;
        if (check_status != NULL)
            return strncmp(recv_buf, check_status, strlen(check_status));
    }
    return 0;
}

/*
 * SMTPサーバーに接続します。
 *
 * server: SMTPサーバーのアドレス
 * port:   SMTPサーバーのポート番号（デフォルトは25）
 *
 * 戻り値
 *  SMTPセッション構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct smtp_session_t* smtp_open(const char* server, ushort port)
{
    struct smtp_session_t* smtp;
    char recv_buf[BUF_SIZE];

    smtp = (struct smtp_session_t*)calloc(1, sizeof(struct smtp_session_t));
    if (smtp == NULL) {
        err_write("smtp_open: no memory.");
        return NULL;
    }
    strncpy(smtp->server, server, sizeof(smtp->server)-1);

    smtp->header = hash_initialize(10);
    if (smtp->header == NULL) {
        free(smtp);
        return NULL;
    }

    smtp->attach_vt = vect_initialize(5);
    if (smtp->attach_vt == NULL) {
        hash_finalize(smtp->header);
        free(smtp);
        return NULL;
    }

    smtp->c_socket = sock_connect_server(server, port);
    if (smtp->c_socket == INVALID_SOCKET) {
        vect_finalize(smtp->attach_vt);
        hash_finalize(smtp->header);
        free(smtp);
        return NULL;
    }

    /* SMTPサーバーからの応答をチェックします。*/
    if (smtp_protocol(smtp, NULL, recv_buf, sizeof(recv_buf), "220") < 0) {
        smtp_close(smtp);
        return NULL;
    }
    smtp->open_session_flag = 1;
    return smtp;
}

/*
 * SMTPサーバーへの接続を終了します。
 *
 * smtp: SMTPセッション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_close(struct smtp_session_t* smtp)
{
    if (smtp == NULL)
        return;

    /* SMTP session end */
    if (smtp->open_session_flag)
        smtp_protocol(smtp, "QUIT\r\n", NULL, 0, NULL);

    /* close socket */
    if (smtp->c_socket != 0)
        SOCKET_CLOSE(smtp->c_socket);

    if (smtp->date)
        free(smtp->date);
    if (smtp->subject)
        free(smtp->subject);
    if (smtp->to)
        free(smtp->to);
    if (smtp->from)
        free(smtp->from);
    if (smtp->cc)
        free(smtp->cc);
    if (smtp->bcc)
        free(smtp->bcc);
    if (smtp->header)
        hash_finalize(smtp->header);
    if (smtp->msg)
        free(smtp->msg);
    if (smtp->attach_vt) {
        int i, n;

        n = vect_count(smtp->attach_vt);
        for (i = 0; i < n; i++) {
            void* d;

            d = vect_get(smtp->attach_vt, i);
            if (d != NULL)
                free(d);
        }
        vect_finalize(smtp->attach_vt);
    }
    free(smtp);
}

/*
 * SMTPセッションに Date: を設定します。
 * すでに Date: が登録されている場合は置換されます。
 *
 * Date: が設定されていない場合は日本標準時間の現在日時を設定します。
 * 日付文字列は以下のフォーマットになります。
 *
 * "Tue, 16 Feb 2010 13:46:48 +0900" (JSTの場合)
 *
 * smtp: SMTPセッション構造体のポインタ
 * date: 日付文字列
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_date(struct smtp_session_t* smtp, const char* date)
{
    if (smtp->date != NULL)
        free(smtp->date);

    smtp->date = (char*)malloc(strlen(date)+1);
    if (smtp->date == NULL) {
        err_write("smtp_set_date: no memory.");
        return;
    }
    strcpy(smtp->date, date);
}

/*
 * SMTPセッションに Subject: を設定します。
 * すでに件名が登録されている場合は置換されます。
 *
 * smtp: SMTPセッション構造体のポインタ
 * subject: 件名
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_subject(struct smtp_session_t* smtp, const char* subject)
{
    if (smtp->subject != NULL)
        free(smtp->subject);

    smtp->subject = (char*)malloc(strlen(subject)+1);
    if (smtp->subject == NULL) {
        err_write("smtp_set_subject: no memory.");
        return;
    }
    strcpy(smtp->subject, subject);
}

/*
 * SMTPセッションに To: を設定します。
 * すでに To: が登録されている場合は置換されます。
 *
 * 宛先が複数の場合はカンマで区切って指定します。
 *
 * smtp: SMTPセッション構造体のポインタ
 * to: 宛先
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_to(struct smtp_session_t* smtp, const char* to)
{
    if (smtp->to != NULL)
        free(smtp->to);

    smtp->to = (char*)malloc(strlen(to)+1);
    if (smtp->to == NULL) {
        err_write("smtp_set_to: no memory.");
        return;
    }
    strcpy(smtp->to, to);
}

/*
 * SMTPセッションに From: を設定します。
 * すでに From: が登録されている場合は置換されます。
 *
 * smtp: SMTPセッション構造体のポインタ
 * from: 送信者
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_from(struct smtp_session_t* smtp, const char* from)
{
    if (smtp->from != NULL)
        free(smtp->from);

    smtp->from = (char*)malloc(strlen(from)+1);
    if (smtp->from == NULL) {
        err_write("smtp_set_from: no memory.");
        return;
    }
    strcpy(smtp->from, from);
}

/*
 * SMTPセッションに Cc: を設定します。
 * すでに Cc: が登録されている場合は置換されます。
 *
 * カーボンコピーの宛先が複数の場合はカンマで区切って指定します。
 *
 * smtp: SMTPセッション構造体のポインタ
 * cc: Cc宛先
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_cc(struct smtp_session_t* smtp, const char* cc)
{
    if (smtp->cc != NULL)
        free(smtp->cc);

    smtp->cc = (char*)malloc(strlen(cc)+1);
    if (smtp->cc == NULL) {
        err_write("smtp_set_cc: no memory.");
        return;
    }
    strcpy(smtp->cc, cc);
}

/*
 * SMTPセッションに Bcc: を設定します。
 * すでに Bcc: が登録されている場合は置換されます。
 *
 * ブラインドカーボンコピーの宛先が複数の場合はカンマで区切って指定します。
 *
 * smtp: SMTPセッション構造体のポインタ
 * bcc: Bcc宛先
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_bcc(struct smtp_session_t* smtp, const char* bcc)
{
    if (smtp->bcc != NULL)
        free(smtp->bcc);

    smtp->bcc = (char*)malloc(strlen(bcc)+1);
    if (smtp->bcc == NULL) {
        err_write("smtp_set_bcc: no memory.");
        return;
    }
    strcpy(smtp->bcc, bcc);
}

/*
 * SMTPセッションに任意のヘッダーを設定します。
 * すでに同じ名前のヘッダーが登録されている場合は置換されます。
 *
 * smtp: SMTPセッション構造体のポインタ
 * name: ヘッダー名
 * value: ヘッダー値
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_header(struct smtp_session_t* smtp, const char* name, const char* value)
{
    hash_put(smtp->header, name, value);
}

/*
 * SMTPセッションにメール本文を設定します。
 * すでにメール本文が設定されている場合は置換されます。
 *
 * メール本文中の改行コードは CRLF とします。
 *
 * smtp: SMTPセッション構造体のポインタ
 * msg: メール本文
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_set_body(struct smtp_session_t* smtp, const char* msg)
{
    if (smtp->msg != NULL)
        free(smtp->msg);

    smtp->msg = (char*)malloc(strlen(msg)+1);
    if (smtp->msg == NULL) {
        err_write("smtp_set_body: no memory.");
        return;
    }
    strcpy(smtp->msg, msg);
}

/*
 * SMTPセッションに添付ファイルを追加します。
 * 添付ファイルを設定するとヘッダーの Content-Type は multipart/mixed になります。
 *
 * エンコードされたデータの領域はメールを送信するまでは解放してはいけません。
 *
 * smtp:              SMTPセッション構造体のポインタ
 * content_type:      添付ファイルの Content-Type を指定
 *                    application/msword; name="退職願.doc"
 * transfer_encoding: エンコーディング種別（base64）
 * file_name:         ファイル名（日本語の場合は base64でエンコードする）
 *                    "退職願.doc"
 * enc_data:          エンコードされたデータ（終端は'\0'）
 *
 * 戻り値
 *  添付ファイルを識別するポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT void* smtp_add_attach(struct smtp_session_t* smtp,
                                const char* content_type,
                                const char* transfer_encoding,
                                const char* file_name,
                                const char* enc_data)
{
    struct smtp_attach_file_t* af;

    af = (struct smtp_attach_file_t*)malloc(sizeof(struct smtp_attach_file_t));
    if (af == NULL) {
        err_write("smtp_add_attach: no memory.");
        return NULL;
    }

    af->content_type = content_type;
    af->transfer_encoding = transfer_encoding;
    af->file_name = file_name;
    af->enc_data = enc_data;

    if (vect_append(smtp->attach_vt, af) < 0) {
        free(af);
        return NULL;
    }
    return af;
}

/*
 * SMTPセッションに設定した添付ファイルを削除します。
 *
 * エンコードされたデータの領域は呼び出し側で解放します。
 *
 * smtp: SMTPセッション構造体のポインタ
 * afid: 添付ファイルを識別するポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void smtp_delete_attach(struct smtp_session_t* smtp, const void* afid)
{
    vect_delete(smtp->attach_vt, afid);
    free((void*)afid);
}

/*
 * "Taro Yamada <foo@bar.com>" からメールアドレスだけを dst の
 * 領域に抽出して返します。
 */
static char* extract_addr(char* dst, int dst_size, const char* src)
{
    int st, ed;
    int size;

    st = indexof(src, '<');
    if (st < 0) {
        strncpy(dst, src, dst_size-1);
        return dst;
    }
    ed = lastindexof(src, '>');
    if (ed < 0) {
        strncpy(dst, src, dst_size-1);
        return dst;
    }
    size = ed - st - 1;
    if (size > dst_size) {
        strncpy(dst, src, dst_size-1);
        return dst;
    }
    return substr(dst, src, st+1, size);
}

/*
 * SMTP の "RCPT TO:" でメールアドレスを設定します。
 * To: Cc: Bcc: の宛先が対象になります。
 */
static int rcpt_to(struct smtp_session_t* smtp, const char* to_buf)
{
    char* buf;
    char** list;
    char** list_ptr;
    char send_buf[BUF_SIZE];
    char recv_buf[BUF_SIZE];

    buf = (char*)alloca(strlen(to_buf)+1);
    strcpy(buf, to_buf);
    list_ptr = list = split(buf, ',');
    while (*list_ptr) {
        char email[256];

        extract_addr(email, sizeof(email), trim(*list_ptr));
        sprintf(send_buf, "RCPT TO:<%s>\r\n", email);
        if (smtp_protocol(smtp, send_buf, recv_buf, sizeof(recv_buf), "250") < 0)
            return -1;
        list_ptr++;
    }
    list_free(list);
    return 0;
}

/*
 * maltipart でメール本文を送信します。
 */
static int send_attach_message(struct smtp_session_t* smtp,
                               const char* boundary_str,
                               const char* content_type,
                               const char* transfer_encoding)
{
    char send_buf[BUF_SIZE];
    char* msg_buf;
    int result;

    sprintf(send_buf, "--%s\r\n", boundary_str);
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;
    if (content_type != NULL) {
        sprintf(send_buf, "Content-Type: %s\r\n", content_type);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }
    if (transfer_encoding != NULL) {
        sprintf(send_buf, "Content-Transfer-Encoding: %s\r\n", transfer_encoding);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }

    /* 区切りの空行(CRLF) */
    strcpy(send_buf, "\r\n");
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    /* メール本文 */
    msg_buf = (char*)malloc(strlen(smtp->msg) + sizeof("\r\n"));
    if (msg_buf == NULL) {
        err_write("smtp_send(): attach message no memory.");
        return -1;
    }
    sprintf(msg_buf, "%s\r\n", smtp->msg);
    result = smtp_protocol(smtp, msg_buf, NULL, 0, NULL);
    free(msg_buf);
    if (result < 0)
        return -1;
    return 0;
}

/*
 * maltipart で添付ファイルを送信します。
 */
static int send_attach_file(struct smtp_session_t* smtp,
                            struct smtp_attach_file_t* af,
                            const char* boundary_str)
{
    char send_buf[BUF_SIZE];
    char* msg_buf;
    int result;

    if (af->enc_data == NULL) {
        err_write("smtp_send(): attach file no data.");
        return -1;
    }

    sprintf(send_buf, "--%s\r\n", boundary_str);
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    if (af->content_type != NULL) {
        sprintf(send_buf, "Content-Type: %s\r\n", af->content_type);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }
    if (af->transfer_encoding != NULL) {
        sprintf(send_buf, "Content-Transfer-Encoding: %s\r\n", af->transfer_encoding);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }
    if (af->file_name != NULL) {
        sprintf(send_buf, "Content-Disposition: attachment; filename=\"%s\"\r\n", af->file_name);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }

    /* 区切りの空行(CRLF) */
    strcpy(send_buf, "\r\n");
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    msg_buf = (char*)malloc(strlen(af->enc_data) + sizeof("\r\n"));
    if (msg_buf == NULL) {
        err_write("smtp_send(): attach file data no memory.");
        return -1;
    }
    sprintf(msg_buf, "%s\r\n", af->enc_data);
    result = smtp_protocol(smtp, msg_buf, NULL, 0, NULL);
    free(msg_buf);
    if (result < 0)
        return -1;
    return 0;
}

/*
 * SMTPプロトコルでメールを送信します。
 * To: と From: 及びメール本文は必須となります。 
 *
 * 添付ファイルが設定されている場合は multipart/mixed 形式で送信します。
 *
 * smtp: SMTPセッション構造体のポインタ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int smtp_send(struct smtp_session_t* smtp)
{
    char local_ip_addr[256];
    char send_buf[BUF_SIZE];
    char recv_buf[BUF_SIZE];
    char email[256];
    char** keys;
    int attach_files;
    char* msg_content_type = NULL;
    char* msg_transfer_encoding = NULL;
    const char* boundary_str = "----------boundary_MULTIPART_MIXED";

    if (smtp->to == NULL || smtp->from == NULL || smtp->msg == NULL) {
        err_write("smtp_send 'To:', 'From:' or 'message' is NULL");
        return -1;
    }

    /* 自身のIPアドレスを取得してSMTPに挨拶します。*/
    if (sock_local_addr(local_ip_addr) == NULL)
        strcpy(local_ip_addr, "localhost");
    sprintf(send_buf, "HELO %s\r\n", local_ip_addr);
    if (smtp_protocol(smtp, send_buf, recv_buf, sizeof(recv_buf), "250") < 0)
        return -1;

    /* SMTPコマンド */
    extract_addr(email, sizeof(email), smtp->from);
    sprintf(send_buf, "MAIL FROM:<%s>\r\n", trim(email));
    if (smtp_protocol(smtp, send_buf, recv_buf, sizeof(recv_buf), "250") < 0)
        return -1;

    if (rcpt_to(smtp, smtp->to) < 0)
        return -1;

    if (smtp->cc != NULL) {
        if (rcpt_to(smtp, smtp->cc) < 0)
            return -1;
    }
    if (smtp->bcc != NULL) {
        if (rcpt_to(smtp, smtp->bcc) < 0)
            return -1;
    }

    /* ヘッダーとメール本文 */
    sprintf(send_buf, "DATA\r\n");
    if (smtp_protocol(smtp, send_buf, recv_buf, sizeof(recv_buf), "354") < 0)
        return -1;

    if (smtp->date == NULL) {
        char date_buf[256];
        sprintf(send_buf, "Date: %s\r\n", now_jststr(date_buf, sizeof(date_buf)));
    } else {
        sprintf(send_buf, "Date: %s\r\n", smtp->date);
    }
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    if (smtp->subject != NULL) {
        sprintf(send_buf, "Subject: %s\r\n", smtp->subject);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }

    sprintf(send_buf, "From: %s\r\n", smtp->from);
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    sprintf(send_buf, "To: %s\r\n", smtp->to);
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    if (smtp->cc != NULL) {
        sprintf(send_buf, "Cc: %s\r\n", smtp->cc);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;
    }

    /* 添付ファイルがあるか調べます。*/
    attach_files = vect_count(smtp->attach_vt);
    if (attach_files > 0) {
        sprintf(send_buf, "Content-Type: multipart/mixed; boundary=\"%s\"\r\n", boundary_str);
        if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
            return -1;

        /* メール本文の Content-Type を取得します。*/
        msg_content_type = (char*)hash_get(smtp->header, "Content-Type");
        msg_transfer_encoding = (char*)hash_get(smtp->header, "Content-Transfer-Encoding");

        /* ヘッダーにある Content-Type は削除します。*/
        if (msg_content_type != NULL)
            hash_delete(smtp->header, "Content-Type");
        if (msg_transfer_encoding != NULL)
            hash_delete(smtp->header, "Content-Transfer-Encoding");
    }

    /* その他のヘッダー */
    keys = hash_keylist(smtp->header);
    if (keys != NULL) {
        int i;

        for (i = 0; keys[i]; i++) {
            char* value;

            value = (char*)hash_get(smtp->header, keys[i]);
            if (value != NULL) {
                sprintf(send_buf, "%s: %s\r\n", keys[i], value);
                if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
                    continue;
            }
        }
        hash_list_free((void**)keys);
    }

    /* 区切りの空行(CRLF) */
    strcpy(send_buf, "\r\n");
    if (smtp_protocol(smtp, send_buf, NULL, 0, NULL) < 0)
        return -1;

    if (attach_files > 0) {
        int i;

        /* メール本文をマルチパートで送信します。*/
        if (send_attach_message(smtp, boundary_str, msg_content_type, msg_transfer_encoding) < 0)
            return -1;

        /* 添付ファイルを送信します。*/
        for (i = 0; i < attach_files; i++) {
            struct smtp_attach_file_t* af;

            af = (struct smtp_attach_file_t*)vect_get(smtp->attach_vt, i);
            if (af == NULL)
                continue;
            if (send_attach_file(smtp, af, boundary_str) < 0)
                return -1;
        }
        /* 添付ファイルのバウンダリの終わりを送信します。*/
        sprintf(send_buf, "--%s--\r\n.\r\n", boundary_str);
        if (smtp_protocol(smtp, send_buf, recv_buf, sizeof(recv_buf), "250") < 0)
            return -1;
    } else {
        char* msg_buf;

        /* メール本文 */
        msg_buf = (char*)alloca(strlen(smtp->msg) + sizeof("\r\n.\r\n"));
        sprintf(msg_buf, "%s\r\n.\r\n", smtp->msg);
        if (smtp_protocol(smtp, msg_buf, recv_buf, sizeof(recv_buf), "250") < 0)
            return -1;
    }
    return 0;
}
