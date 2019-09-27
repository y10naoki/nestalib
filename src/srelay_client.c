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

#define WAIT_RECV_TIMEOUT  300   /* ms */

/* セッション・リレーサーバー情報 */
struct rserver_info_t {
    int is_active;
    char* host;
    ushort port;
};

/* セッション・リレーサーバーを管理するデータ(read only) */
static struct srelay_server_t g_rsvr;

static int send_cmd_hello_server(SOCKET socket)
{
    int result = -1;

    /* コマンドの送信 */
    if (send_data(socket, "HS", 2) < 0)
        return -1;

    /* サーバーからの応答を受信します。*/
    if (wait_recv_data(socket, WAIT_RECV_TIMEOUT)) {
        char buf[2];
        int status;

        /* "OK"を受信します。*/
        recv_char(socket, buf, sizeof(buf), &status);
        if (status == 0 && memcmp(buf, "OK", 2) == 0)
            result = 0;
    }
    return result;
}

/* セッション・リレー・サーバーの生存を監視するスレッド */
static void rserver_check_thread(void* argv)
{
    int stime;
    struct srelay_server_t* rsvr;

    rsvr = (struct srelay_server_t*)argv;
    stime = rsvr->check_interval_time;

#ifdef _WIN32
    /* windowsのSleep()はミリ秒になります。*/
    stime *= 1000;
#endif

    while (1) {
        struct rserver_info_t** rs_list;

        /* 指定時間眠りに入りますzzz。*/
        sleep(stime);

        /* 終了フラグが真の場合はスレッドを終了します。*/
        if (rsvr->thread_end_flag)
            break;

        /* ハッシュテーブルのデータポインタをリストとして取得します。*/
        rs_list = (struct rserver_info_t**)hash_list(rsvr->rs_tbl);
        if (rs_list) {
            int i = 0;

            while (rs_list[i]) {
                struct rserver_info_t* rs;
                SOCKET socket;

                /* リレー・サーバーが正常に動作しているか調べます。*/
                rs = rs_list[i];
                rs->is_active = 0;
                socket = sock_connect_server(rs->host, rs->port);
                if (socket != INVALID_SOCKET) {
                    /* チェックのためにコマンドを送信します。*/
                    if (send_cmd_hello_server(socket) == 0)
                        rs->is_active = 1;   /* Okay */
                    SOCKET_CLOSE(socket);
                }
                i++;
            }
            /* ハッシュテーブルのリストを解放します。*/
            hash_list_free((void**)rs_list);
        }
    }
#ifdef _WIN32
    _endthread();
#endif
}

static int send_length_string(SOCKET socket, const char* str)
{
    short len;

    /* サイズを送信します。*/
    len = (short)strlen(str);
    if (send_short(socket, len) < 0)
        return -1;
    /* 内容を送信します。*/
    if (send_data(socket, str, len) < 0)
        return -1;
    return 0;
}

static int send_cmd(SOCKET socket, const char* cmd, const char* zone, const char* skey)
{
    /* コマンドの送信 */
    if (send_data(socket, cmd, (int)strlen(cmd)) < 0)
        return -1;
    /* ゾーン名を送信します。*/
    if (send_length_string(socket, zone) < 0)
        return -1;
    /* セッションキー名を送信します。*/
    if (send_length_string(socket, skey) < 0)
        return -1;
    return 0;
}

static int send_session_copy_server(SOCKET socket, struct session_copy_t* s_cp)
{
    int i;

    /* サーバー数を送信します。*/
    if (send_short(socket, (short)s_cp->count) < 0)
        return -1;
    for (i = 0; i < s_cp->count; i++) {
        char server_name[MAX_HOSTNAME];

        /* サーバー名を送信します。*/
        mt_inet_addr(s_cp->addr[i], server_name);
        if (send_length_string(socket, server_name) < 0)
            return -1;
        /* ポート番号を送信します。*/
        if (send_short(socket, s_cp->port[i]) < 0)
            return -1;
    }
    return 0;
}

static short count_session_data(struct session_t* s,
                                const char** list)
{
    int n = 0;

    while (*list) {
        struct session_data_t* sdata;

        /* セッションデータ構造体 */
        sdata = (struct session_data_t*)hash_get(s->sdata, *list);
        if (sdata != NULL) {
            /* サイズが1以上の場合のみカウントします。*/
            if (sdata->size > 0)
                n++;
        }
        list++;
    }
    return (short)n;
}

static struct rserver_info_t* add_server(const char* host, ushort port)
{
    struct rserver_info_t* rs;

    rs = (struct rserver_info_t*)malloc(sizeof(struct rserver_info_t) +
                                        strlen(host) + 1);
    if (rs == NULL) {
        err_write("add_server: no memory");
        return NULL;
    }
    rs->host = (char*)rs + sizeof(struct rserver_info_t);
    strcpy(rs->host, host);
    rs->port = port;
    rs->is_active = 1;

    if (hash_put(g_rsvr.rs_tbl, host, rs) != 0) {
        free(rs);
        return NULL;
    }
    return rs;
}

static int is_active_server(const char* host, ushort port)
{
    struct rserver_info_t* rs;

    rs = (struct rserver_info_t*)hash_get(g_rsvr.rs_tbl, host);
    if (rs == NULL) {
        /* 存在しない場合は登録します。*/
        rs = add_server(host, port);
        if (rs == NULL)
            return 0;
    }
    return rs->is_active;
}

static void set_passive_server(const char* host, ushort port)
{
    struct rserver_info_t* rs;

    rs = (struct rserver_info_t*)hash_get(g_rsvr.rs_tbl, host);
    if (rs)
        rs->is_active = 0;
}

static SOCKET connect_with_failover(const char* host,
                                    ushort port,
                                    struct session_copy_t* s_cp)
{
    SOCKET socket = INVALID_SOCKET;

    if (is_active_server(host, port)) {
        /* アクティブの場合にサーバーに接続します。*/
        socket = sock_connect_server(host, port);
        if (socket == INVALID_SOCKET)
            set_passive_server(host, port);
    }
    if (socket == INVALID_SOCKET) {
        /* エラーの場合にはコピーを持つサーバーに問い合わせます。*/
        if (s_cp) {
            int i;

            for (i = 0; i < s_cp->count; i++) {
                char hostname[MAX_HOSTNAME];

                mt_inet_addr(s_cp->addr[i], hostname);
                if (is_active_server(hostname, s_cp->port[i])) {
                    socket = sock_connect_server(hostname, s_cp->port[i]);
                    if (socket != INVALID_SOCKET)
                        break;
                    set_passive_server(hostname,  s_cp->port[i]);
                }
            }
        }
    }
    return socket;
}

/*
 * セッション・リレー・サーバーの情報を初期化します。
 *
 * count: コピーするサーバー数
 * host_tbl: コピーするサーバー名の配列
 * port_tbl: コピーするサーバーのポート番号の配列
 * check_interval_time: チェックする間隔（秒数）
 * host_addr: 自身のアドレス
 * host_port: 自身のポート番号
 *
 * 戻り値
 *  セッション・リレーサーバー構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct srelay_server_t* srelay_initialize(int count,
                                                    const char* host_tbl[],
                                                    ushort port_tbl[],
                                                    int check_interval_time,
                                                    ulong host_addr,
                                                    ushort host_port)
{
    int n = 10;
    int i;

    if (count > MAX_SESSION_RELAY_COPY) {
        err_write("srelay_initialize: count over max number is %d", MAX_SESSION_RELAY_COPY);
        return NULL;
    }

    memset(&g_rsvr, '\0', sizeof(struct srelay_server_t));

    g_rsvr.rs_tbl = hash_initialize(n);
    if (g_rsvr.rs_tbl == NULL) {
        err_write("srelay_initialize: no memory");
        return NULL;
    }
    for (i = 0; i < count; i++) {
        /* この時点でサーバーへの接続チェックは行いません。*/
        add_server(host_tbl[i], port_tbl[i]);
    }
    g_rsvr.check_interval_time = check_interval_time;
    g_rsvr.thread_end_flag = 0;

    /* 自身のサーバー情報 */
    g_rsvr.host_addr = host_addr;
    g_rsvr.host_port = host_port;

    /* セッションをコピーするサーバーの情報 */
    g_rsvr.s_cp.count = count;
    for (i = 0; i < count; i++) {
        g_rsvr.s_cp.addr[i] = inet_addr(host_tbl[i]);
        g_rsvr.s_cp.port[i] = port_tbl[i];
    }

    /* スレッドを起動します。*/
    {
#ifdef _WIN32
        uintptr_t rserver_check_thread_id;
        rserver_check_thread_id = _beginthread(rserver_check_thread, 0, &g_rsvr);
#else
        pthread_t rserver_check_thread_id;
        pthread_create(&rserver_check_thread_id, NULL, (void*)rserver_check_thread, &g_rsvr);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(rserver_check_thread_id);
#endif
    }
    return &g_rsvr;
}

/*
 * セッション・リレー・サーバーの情報を開放します。
 *
 * 戻り値
 *  なし
 */
APIEXPORT void srelay_finalize(struct srelay_server_t* rsvr)
{
    struct rserver_info_t** rs_list;

    if (rsvr != &g_rsvr)
        err_write("srelay_finalize: illegal srelay_server_t*");

    g_rsvr.thread_end_flag = 1;

    /* ハッシュテーブルのデータポインタを解放します。*/
    rs_list = (struct rserver_info_t**)hash_list(g_rsvr.rs_tbl);
    if (rs_list) {
        int i = 0;

        while (rs_list[i]) {
            free(rs_list[i]);
            i++;
        }
        /* ハッシュテーブルのリストを解放します。*/
        hash_list_free((void**)rs_list);
    }
    hash_finalize(g_rsvr.rs_tbl);
}

/*
 * セッションを生成したサーバーからセッション情報を転送して
 * セッション構造体に設定します。
 *
 * 正常に転送が完了するとオーナー権が設定されます。
 *
 * s: セッション構造体
 * skey: セッションキー
 * zone: ゾーン名
 * host: セッションを生成したホスト名
 * port: セッションを生成したポート番号
 * owner_host: 新オーナーのホスト名
 * owner_port: 新オーナーのポート番号
 * owner_s_cp: 新オーナーのコピーを持つサーバーの情報
 * s_cp_failover: セッションを生成したサーバーのコピーを持つサーバーの情報
 *
 * 戻り値
 *   正常に終了した場合はゼロを返します。
 *   エラーの場合は -1 を返します。
 */
APIEXPORT int srelay_get_session(struct session_t* s,
                                 const char* skey,
                                 const char* zone,
                                 const char* host,
                                 ushort port,
                                 const char* owner_host,
                                 ushort owner_port,
                                 struct session_copy_t* owner_s_cp,
                                 struct session_copy_t* s_cp_failover)
{
    SOCKET socket;
    int result = -1;

    /* サーバーに接続します。*/
    socket = connect_with_failover(host, port, s_cp_failover);
    if (socket == INVALID_SOCKET)
        return -1;

    /* コマンド RS の送信（ゾーン名とセッションキー） */
    if (send_cmd(socket, "RS", zone, skey) < 0)
        goto final;

    /* 新オーナーホスト名を送信します。*/
    if (send_length_string(socket, owner_host) < 0)
        goto final;

    /* 新オーナーのポート番号を送信します。*/
    if (send_short(socket, owner_port) < 0)
        goto final;

    /* 新オーナーのセッションのコピーを保持しているサーバー情報を送信します。*/
    if (send_session_copy_server(socket, owner_s_cp) < 0)
        goto final;

    /* サーバーからの応答を受信します。*/
    if (wait_recv_data(socket, WAIT_RECV_TIMEOUT)) {
        int status;
        int64 ts;
        short n;

        /* セッションデータをクリアします。*/
        ssn_delete_all(s);

        /* タイムスタンプを取得します。*/
        ts = recv_int64(socket, &status);
        if (status != 0)
            goto final;
        s->last_update = ts;

        /* 個数を取得します。*/
        n = recv_short(socket, &status);
        if (status != 0)
            goto final;

        /* 個数分のセッション名と値を取得します。*/
        while (n--) {
            short len;
            char* key;
            int size;
            void* data;

            /* 名称のlengthを取得します。*/
            len = recv_short(socket, &status);
            if (status != 0)
                goto final;
            if (len < 1)
                goto final;

            /* 名称を取得します。*/
            key = (char*)alloca(len+1);
            recv_char(socket, key, len, &status);
            if (status != 0)
                goto final;
            key[len] = '\0';

            /* 値のlengthを取得します。*/
            size = recv_short(socket, &status);
            if (status != 0)
                goto final;
            if (size < 1)
                goto final;

            /* 値を取得します。*/
            data = malloc(size);
            if (data == NULL) {
                err_write("srelay_get_session(): no memory");
                goto final;
            }
            recv_char(socket, data, size, &status);
            if (status != 0) {
                free(data);
                goto final;
            }

            /* セッション情報を置換します。*/
            ssn_put_nolock(s, key, data, size);
            free(data);
        }
    }

    /* オーナーフラグを設定します。*/
    s->owner_flag = 1;
    result = 0;  /* success */

final:
    SOCKET_CLOSE(socket);
    return result;
}

/*
 * サーバーにセッションのタイムスタンプを問い合わせます。
 *
 * サーバーがダウンしていた場合はセッションのコピーを持つサーバーに
 * 問い合わせます。
 *
 * skey: セッションキー
 * zone: ゾーン名
 * host: ホスト名
 * port: ポート番号
 * s_cp_failover: セッションのコピーを持つサーバーの情報
 *
 * 戻り値
 *   正常に終了した場合はタイムスタンプ値を返します。
 *   エラーの場合はゼロを返します。
 */
APIEXPORT int64 srelay_timestamp(const char* skey,
                                 const char* zone,
                                 const char* host,
                                 ushort port,
                                 struct session_copy_t* s_cp_failover)
{
    SOCKET socket;
    int64 ts = 0;

    /* サーバーに接続します。*/
    socket = connect_with_failover(host, port, s_cp_failover);
    if (socket == INVALID_SOCKET)
        return 0;

    /* コマンド QT の送信（ゾーン名とセッションキー） */
    if (send_cmd(socket, "QT", zone, skey) < 0)
        goto final;

    /* サーバーからの応答を受信します。*/
    if (wait_recv_data(socket, WAIT_RECV_TIMEOUT)) {
        int status;

        /* タイムスタンプ値を受信します。*/
        recv_char(socket, (char*)&ts, sizeof(ts), &status);
        if (status != 0)
            ts = 0;
    }

final:
    SOCKET_CLOSE(socket);
    return ts;
}

/*
 * サーバーにオーナー移行のコマンドを送信します。
 *
 * 自分自身がオーナーとなります。
 *
 * s: セッション構造体
 * skey: セッションキー
 * zone: ゾーン名
 * host: ホスト名
 * port: ポート番号
 * owner_host: 新オーナーのホスト名
 * owner_port: 新オーナーのポート番号
 *
 * 戻り値
 *   正常に終了した場合はゼロを返します。
 *   エラーの場合は -1 を返します。
 */
APIEXPORT int srelay_change_owner(struct session_t* s,
                                  const char* skey,
                                  const char* zone,
                                  const char* host,
                                  ushort port,
                                  const char* owner_host,
                                  ushort owner_port)
{
    SOCKET socket = INVALID_SOCKET;
    int result = -1;

    /* サーバーに接続します。*/
    if (is_active_server(host, port)) {
        socket = sock_connect_server(host, port);
        if (socket == INVALID_SOCKET)
            set_passive_server(host, port);
    }
    if (socket == INVALID_SOCKET)
        return -1;

    /* コマンド CO の送信（ゾーン名とセッションキー） */
    if (send_cmd(socket, "CO", zone, skey) < 0)
        goto final;

    /* 新オーナーホスト名を送信します。*/
    if (send_length_string(socket, owner_host) < 0)
        goto final;

    /* 新オーナーのポート番号を送信します。*/
    if (send_short(socket, owner_port) < 0)
        goto final;

    /* 新オーナーのセッションのコピーを保持しているサーバー情報を送信します。*/
    if (send_session_copy_server(socket, &s->zs->rsvr->s_cp) < 0)
        goto final;

    /* 応答データはなし */

    /* オーナーフラグを設定します。*/
    s->owner_flag = 1;
    result = 0;

final:
    SOCKET_CLOSE(socket);
    return result;
}

/*
 * サーバーからセッション情報を削除します。
 *
 * skey: セッションキー
 * zone: ゾーン名
 * host: ホスト名
 * port: ポート番号
 *
 * 戻り値
 *   正常に終了した場合はゼロを返します。
 *   エラーの場合は -1 を返します。
 */
APIEXPORT int srelay_delete_session(const char* skey,
                                    const char* zone,
                                    const char* host,
                                    ushort port)
{
    SOCKET socket = INVALID_SOCKET;
    int result;

    /* サーバーに接続します。*/
    if (is_active_server(host, port)) {
        socket = sock_connect_server(host, port);
        if (socket == INVALID_SOCKET)
            set_passive_server(host, port);
    }
    if (socket == INVALID_SOCKET)
        return -1;

    /* コマンド DS の送信（ゾーン名とセッションキー） */
    result = send_cmd(socket, "DS", zone, skey);

    /* 応答データはなし */
    SOCKET_CLOSE(socket);
    return result;
}

/*
 * コピーを持つサーバーにセッションを転送します。
 *
 * s: セッション構造体
 * skey: セッションキー
 * zone: ゾーン名
 * host: セッションをコピーするホスト名
 * port: セッションをコピーするポート番号
 * owner_host: 現オーナーのホスト名
 * owner_port: 現オーナーのポート番号
 * s_cp: 現オーナーのコピーを持つサーバーの情報
 *
 * 戻り値
 *   正常に終了した場合はゼロを返します。
 *   エラーの場合は -1 を返します。
 */
APIEXPORT int srelay_copy_session(struct session_t* s,
                                  const char* skey,
                                  const char* zone,
                                  const char* host,
                                  ushort port,
                                  const char* owner_host,
                                  ushort owner_port,
                                  struct session_copy_t* owner_s_cp)
{
    SOCKET socket = INVALID_SOCKET;
    char** key_list = NULL;
    short sc = 0;
    short i;
    int result = -1;

    /* サーバーに接続します。*/
    if (is_active_server(host, port)) {
        socket = sock_connect_server(host, port);
        if (socket == INVALID_SOCKET)
            set_passive_server(host, port);
    }
    if (socket == INVALID_SOCKET)
        return -1;

    /* コマンド CS の送信（ゾーン名とセッションキー） */
    if (send_cmd(socket, "CS", zone, skey) < 0)
        goto final;

    /* セッション識別子を送信します。*/
    if (send_length_string(socket, s->sid) < 0)
        goto final;

    /* オーナーホスト名を送信します。*/
    if (send_length_string(socket, owner_host) < 0)
        goto final;

    /* オーナーのポート番号を送信します。*/
    if (send_short(socket, owner_port) < 0)
        goto final;

    /* オーナーのセッションのコピーを保持しているサーバー情報を送信します。*/
    if (send_session_copy_server(socket, owner_s_cp) < 0)
        goto final;

    if (s->sdata != NULL) {
        /* セッションのキーを列挙します。*/
        key_list = hash_keylist(s->sdata);
        if (key_list != NULL) {
            /* セッションのキー数を求めます。
               値のサイズがゼロのものは対象外とします。*/
            sc = count_session_data(s, (const char**)key_list);
        }
    }

    /* タイムスタンプを送信します。*/
    if (send_int64(socket, s->last_update) < 0)
        goto final;

    /* セッションのキー数を送信します。*/
    if (send_short(socket, sc) < 0)
        goto final;

    /* キーと値のペアを送信します。*/
    for (i = 0; i < sc; i++) {
        short len;
        struct session_data_t* sdata;

        /* キーのlengthを送信します。*/
        len = (short)strlen(key_list[i]);
        if (send_short(socket, len) < 0)
            goto final;

        /* キーを送信します。*/
        if (send_data(socket, key_list[i], len) < 0)
            goto final;

        /* セッションデータ構造体 */
        sdata = (struct session_data_t*)hash_get(s->sdata, key_list[i]);
        if (sdata == NULL)
            goto final;
        if (sdata->size > 0) {
            /* 値のlengthを送信します。*/
            if (send_short(socket, (short)sdata->size) < 0)
                goto final;
            /* 値を送信します。*/
            if (send_data(socket, sdata->data, sdata->size) < 0)
                goto final;
        }
    }
    result = 0;

final:
    if (key_list != NULL)
        hash_list_free((void**)key_list);
    if (result < 0)
        err_write("srelay_copy_session(): session send error(%s).", strerror(errno));

    SOCKET_CLOSE(socket);
    return result;
}
