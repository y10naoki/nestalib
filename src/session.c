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

#define IPADDR_SIZE      10
#define PORTNO_SIZE      5
#define ZONENAME_SIZE    2
#define COPYSERVER_SIZE  2

#define INIT_SESSION_CAPACITY 100    /* 個別セッション領域の大きさ */

/*
 * セッション管理の関数群です。
 * スレッドセーフで動作します。
 *
 * セッション識別子の最大サイズは 255バイトです。
 *
 * セッション識別子は以下の構造で作成されます。
 *
 * セッション・リレーを使用しない場合(cbhostがNULL)の場合は
 * IPアドレスバイナリ以下は格納されません。
 *
 * セッションのコピーを行わない場合は、
 * セッションのコピーを持つサーバー数(*1)以下は格納されません。
 *
 *   +------------------------------------------------+
 *   |セッション構造体のアドレス:MD5(32バイト)        |
 *   +------------------------------------------------+
 *   |IPアドレスバイナリ(10バイト)                    |
 *   +------------------------------------------------+
 *   |ポート番号バイナリ(5バイト)                     |
 *   +------------------------------------------------+
 *   |ゾーン名のバイト数(2バイト)                     |
 *   +------------------------------------------------+
 *   |ゾーン名(nバイト)                               |
 *   +------------------------------------------------+
 *   |セッションのコピーを持つサーバー数(*1)(2バイト) |
 *   +------------------------------------------------+
 *   |  コピーを持つIPアドレスバイナリ(10バイト)      |
 *   +------------------------------------------------+
 *   |  コピーを持つポート番号バイナリ(5バイト)       |
 *   +------------------------------------------------+
 *   |  [サーバー数分繰り返す]                        |
 *   +------------------------------------------------+
 *
 * 内部ではハッシュテーブルを使用して複数のセッションを管理します。
 * ハッシュテーブルで管理されるキーは、セッション構造体のアドレスを
 * MD5 でハッシュした32バイトの文字列になります。
 * なお、セッションリレーが有効の場合はセッション構造体のアドレスに
 * IPアドレスを付加した値を MD5 でハッシュしたキーを使用します。
 * これはセッションキーが別のサーバー上でもユニークになるための措置です。
 */

/* セッション管理テーブルのキーを作成します。*/
static char* create_session_key(char* skey,
                                struct zone_session_t* zs,
                                struct session_t* s)
{
    char saddr[128];

    snprintf(saddr, sizeof(saddr), "%lu", (ulong)s);
    if (zs->rsvr) {
        char tbuf[20];

        snprintf(tbuf, sizeof(tbuf), "%lu", zs->rsvr->host_addr);
        strcat(saddr, tbuf);
    }
    md5(skey, saddr);
    return skey;
}

/* セッション識別子 sid の生成 */
static char* create_session_id(char* sid,
                               const char* skey,
                               struct zone_session_t* zs)
{
    char abuf[64];
    char hexzone[MAX_ZONENAME*2];
    ushort size;
    char hexsize[10];

    strcpy(sid, skey);

    if (zs->rsvr == NULL)
        return sid;

    /* ホストIPアドレス（バイナリ値）を10進数表記に変換して
       セッション識別子に加えます。 */
    snprintf(abuf, sizeof(abuf), "%010ld", zs->rsvr->host_addr);
    strcat(sid, abuf);

    /* ホストポート番号（バイナリ値）を10進数表記に変換して
       セッション識別子に加えます。 */
    snprintf(abuf, sizeof(abuf), "%05d", zs->rsvr->host_port);
    strcat(sid, abuf);

    /* ゾーン名を16進数文字列に変換します。*/
    tohex(hexzone, zs->zone_name, (int)strlen(zs->zone_name));

    /* ゾーン名のサイズを16進数文字列に変換します。*/
    size = (ushort)strlen(hexzone);
    snprintf(hexsize, sizeof(hexsize), "%02d", size);

    strcat(sid, hexsize);
    strcat(sid, hexzone);

    if (zs->rsvr->s_cp.count > 0) {
        int i;

        /* セッションをコピーするサーバーの数 */
        snprintf(abuf, sizeof(abuf), "%02d", zs->rsvr->s_cp.count);
        strcat(sid, abuf);

        /* セッションをコピーするIPアドレスとポート番号 */
        for (i = 0; i < zs->rsvr->s_cp.count; i++) {
            snprintf(abuf, sizeof(abuf), "%010ld", zs->rsvr->s_cp.addr[i]);
            strcat(sid, abuf);

            snprintf(abuf, sizeof(abuf), "%05d", zs->rsvr->s_cp.port[i]);
            strcat(sid, abuf);
        }
    }
    return sid;
}

/* セッション識別子からセッションのキーを取り出します。*/
static int sid_to_skey(struct zone_session_t* zs,
                       const char* sid,
                       char* skey)
{
    int len;

    len = (int)strlen(sid);
    if (len < SESSION_KEY_SIZE) {
        err_write("session: illegal sid=%s.", sid);
        return -1;
    }

    memcpy(skey, sid, SESSION_KEY_SIZE);
    skey[SESSION_KEY_SIZE] = '\0';
    return 0;
}

static ulong extract_ulong(const char* p, int size)
{
    char buf[256];

    memcpy(buf, p, size);
    buf[size] = '\0';
    return (ulong)atol(buf);
}

static ushort extract_ushort(const char* p, int size)
{
    char buf[256];

    memcpy(buf, p, size);
    buf[size] = '\0';
    return (ushort)atoi(buf);
}

/* セッション識別子から生成元のIPアドレス（バイナリ）とポート番号を取り出します。
   また、生成元のコピーを持つサーバーの情報も取得します。*/
static ulong sid_to_ip(struct zone_session_t* zs,
                       const char* sid,
                       ushort* port_no,
                       struct session_copy_t* s_cp)
{
    int len;

    len = (int)strlen(sid);
    if (len < SESSION_KEY_SIZE + IPADDR_SIZE + PORTNO_SIZE) {
        err_write("session: illegal sid=%s.", sid);
        return 0;
    }

    /* コピーを持つサーバーの情報をクリアします。*/
    memset(s_cp, '\0', sizeof(struct session_copy_t));

    if (zs->rsvr) {
        char* tp;
        ulong host_addr;
        ushort zone_size;

        /* ホストIPアドレスを取り出します。*/
        tp = (char*)&sid[SESSION_KEY_SIZE];
        host_addr = extract_ulong(tp, IPADDR_SIZE);
        tp += IPADDR_SIZE;

        /* ポート番号を取り出します。*/
        *port_no = extract_ushort(tp, PORTNO_SIZE);
        tp += PORTNO_SIZE;

        /* ゾーン名サイズ */
        zone_size = extract_ushort(tp, ZONENAME_SIZE);
        tp += ZONENAME_SIZE;

        /* ゾーン名 */
        tp += zone_size;

        if (*tp && strlen(tp) >= COPYSERVER_SIZE) {
            /* コピーを持つサーバーの情報を取り出します。*/
            ushort cp_count;

            /* コピーを持つサーバー数 */
            cp_count = extract_ushort(tp, COPYSERVER_SIZE);
            tp += COPYSERVER_SIZE;

            if (cp_count <= MAX_SESSION_RELAY_COPY) {
                int i;

                for (i = 0; i < cp_count; i++) {
                    /* IPアドレスを取り出します。*/
                    s_cp->addr[i] = extract_ulong(tp, IPADDR_SIZE);
                    tp += IPADDR_SIZE;

                    /* ポート番号を取り出します。*/
                    s_cp->port[i] = extract_ushort(tp, PORTNO_SIZE);
                    tp += PORTNO_SIZE;
                }
                s_cp->count = cp_count;
            }
        }
        return host_addr;
    }
    return 0;
}

/* セッション識別子からゾーン名を取り出します。*/
static void sid_to_zone(struct zone_session_t* zs,
                        const char* sid,
                        char* zname)
{
    int len;

    *zname = '\0';
    len = (int)strlen(sid);
    if (len < SESSION_KEY_SIZE + IPADDR_SIZE + PORTNO_SIZE + ZONENAME_SIZE) {
        err_write("session: illegal sid=%s.", sid);
        return;
    }

    if (zs->rsvr) {
        char* tp;
        char szbuf[ZONENAME_SIZE+1];
        int zsize;
        char zhex[MAX_ZONENAME*2];

        tp = (char*)&sid[SESSION_KEY_SIZE+IPADDR_SIZE+PORTNO_SIZE];
        memcpy(szbuf, tp, ZONENAME_SIZE);
        szbuf[ZONENAME_SIZE] = '\0';
        zsize = atoi(szbuf);
        if (zsize > MAX_ZONENAME-1) {
            err_write("session: illegal sid(zonename)=%s.", sid);
            return;
        }
        memcpy(zhex, tp+ZONENAME_SIZE, zsize);
        zhex[zsize] = '\0';
        tochar(zname, zhex);
    }
}

static struct session_t* new_session(struct zone_session_t* zs)
{
    struct session_t* s;

    if (zs->max_session != SESSION_UNLIMITED) {
        /* セッション数の制限チェック */
        if (zs->cur_session >= zs->max_session) {
            err_write("session: max session[%d] over.", zs->max_session);
            return NULL;
        }
    }

    s = (struct session_t*)calloc(1, sizeof(struct session_t));
    if (s == NULL) {
        err_write("session: session_create() no memory.");
        return NULL;
    }

    /* 個別セッションの領域を確保します。*/
    s->sdata = hash_initialize(INIT_SESSION_CAPACITY);
    if (s->sdata == NULL) {
        err_write("session: copy_session_create() no memory.");
        free(s);
        return NULL;
    }

    s->zs = zs;
    s->last_access = s->last_update = system_time();
    return s;
}

/* セッション・リレーで生成元の複製セッションを作成します。*/
static struct session_t* copy_session_create(struct zone_session_t* zs,
                                             const char* sid,
                                             const char* skey)
{
    struct session_t* s = NULL;

    s = new_session(zs);
    if (s == NULL)
        return NULL;

    /* セッション管理テーブルにセッションを登録します。*/
    if (hash_put(zs->s_tbl, skey, s) < 0) {
        free(s);
        return NULL;
    }
    zs->cur_session++;

    strcpy(s->sid, sid);
    strcpy(s->skey, skey);
    return s;
}

/* セッションのタイムスタンプをチェックしてオーナー権を取得します。*/
static int get_session_failover(struct session_t* s,
                                const char* skey,
                                const char* zname,
                                const char* hostname,
                                ushort hostport,
                                const char* my_hostname,
                                ushort my_port,
                                struct session_copy_t* my_s_cp,
                                struct session_copy_t* owner_s_cp)
{
    int64 ts;

    ts = srelay_timestamp(skey, zname, hostname, hostport, owner_s_cp);
    if (ts == 0)
        return -1;
    if (ts > s->last_update) {
        /* 自分のセッションは古いので最新の内容を要求します。
           オーナー権も移行されます。*/
        if (srelay_get_session(s, skey, zname, hostname, hostport, my_hostname, my_port, my_s_cp, owner_s_cp) < 0)
            return -1;
    } else {
        int i;

        /* 最新を取得しているのでオーナー権だけを取得します。*/
        /* オーナー権が移動することを現オーナーのコピーを持つサーバーに通知します。*/
        for (i = 0; i < owner_s_cp->count; i++) {
            char owner_host[MAX_HOSTNAME];

            mt_inet_addr(owner_s_cp->addr[i], owner_host);
            srelay_change_owner(s, skey, zname, owner_host, owner_s_cp->port[i], my_hostname, my_port);
        }
        /* オーナー権を取得します。*/
        if (srelay_change_owner(s, skey, zname, hostname, hostport, my_hostname, my_port) < 0)
            return -1;
    }
    return 0;
}

/* セッション・リレーでオーナー権を取得します。*/
static struct session_t* get_session_owner(struct zone_session_t* zs,
                                           const char* sid,
                                           const char* skey,
                                           struct session_t* s)
{
    ulong hostip;
    char hostname[MAX_HOSTNAME];
    ushort hostport;
    char zname[MAX_ZONENAME];
    char my_hostname[MAX_HOSTNAME];
    struct session_copy_t failover_s_cp;

    /* sidからセッションの生成元のIPアドレスを取得します。*/
    hostip = sid_to_ip(zs, sid, &hostport, &failover_s_cp);
    if (hostip == 0)
        return NULL;

    /* sidからゾーン名を取得します。*/
    sid_to_zone(zs, sid, zname);

    /* 自身のホスト名（IPアドレス）を取得します。*/
    mt_inet_addr(zs->rsvr->host_addr, my_hostname);

    if (hostip == zs->rsvr->host_addr) {
        /* 自分が生成したセッションの場合 */
        if (s != NULL) {
            if (! s->owner_flag) {
                /* オーナーではない場合に現在のオーナーに問い合わせます。*/
                mt_inet_addr(s->owner_addr, hostname);
                /* セッションとオーナー権を取得します。*/
                if (get_session_failover(s,
                                         skey,
                                         zname,
                                         hostname,
                                         s->owner_port,
                                         my_hostname,
                                         zs->rsvr->host_port,
                                         &zs->rsvr->s_cp,
                                         &s->owner_s_cp) != 0)
                    return NULL;
            }
        }
    } else {
        /* 自分が生成したセッションでない場合のリレーを行ないます。*/

        /* 生成元のホスト名（IPアドレス）を取得します。*/
        mt_inet_addr(hostip, hostname);

        if (s == NULL) {
            /* セッションが存在しない場合 */

            /* セッションの複製を作成します。*/
            s = copy_session_create(zs, sid, skey);
            if (s == NULL)
                return NULL;
            /* セッションを生成元のサーバーから取得します。
               オーナー権も移行されます。*/
            if (get_session_failover(s,
                                     skey,
                                     zname,
                                     hostname,
                                     hostport,
                                     my_hostname,
                                     zs->rsvr->host_port,
                                     &zs->rsvr->s_cp,
                                     &failover_s_cp) != 0) {
                /* エラーの場合に領域を開放します。*/
                ssn_free_nolock(s);
                hash_delete(zs->s_tbl, skey);
                return NULL;
            }
        } else {
            /* セッションが存在する場合 */
            if (! s->owner_flag) {
                /* オーナーではない場合に生成元から取得します。*/
                if (get_session_failover(s,
                                         skey,
                                         zname,
                                         hostname,
                                         hostport,
                                         my_hostname,
                                         zs->rsvr->host_port,
                                         &zs->rsvr->s_cp,
                                         &failover_s_cp) != 0)
                    return NULL;
            }
        }
    }
    return s;
}

static int is_own_session(struct zone_session_t* zs, const char* sid)
{
    ulong hostip;
    ushort hostport;
    struct session_copy_t failover_s_cp;

    hostip = sid_to_ip(zs, sid, &hostport, &failover_s_cp);
    if (hostip == 0)
        return 0;
    return (hostip == zs->rsvr->host_addr);
}

/* セッションタイムアウトを監視するスレッド */
static void session_timeout_thread(void* argv)
{
    struct zone_session_t* zs;
    int stime;

    zs = (struct zone_session_t*)argv;
    if (zs->timeout <= 0)
        return;

    /* 指定されたタイムアウト（秒）の1/2の間スリープします。*/
    stime = zs->timeout / 2;
#ifdef _WIN32
    /* windowsのSleep()はミリ秒になります。*/
    stime *= 1000;
#endif

    while (1) {
        struct session_t** s_list;
        int64 now_time;

        /* 指定時間眠りに入りますzzz。*/
        sleep(stime);

        /* 終了フラグが真の場合はスレッドを終了します。*/
        if (zs->thread_end_flag)
            break;

        /* タイムアウトしているセッションがあるか調べて削除します。*/
        CS_START(&zs->critical_section);
        now_time = system_time();

        /* ハッシュテーブルのデータポインタをリストとして取得します。*/
        s_list = (struct session_t**)hash_list(zs->s_tbl);
        if (s_list == NULL) {
            err_write("session: timeout thread no memory.");
        } else {
            int i = 0;

            while (s_list[i]) {
                struct session_t* s;
                int elap;

                s = s_list[i];
                elap = (int)((now_time - s->last_access) / 1000000L);
                if (elap > zs->timeout) {
                    /* タイムアウトしているので削除します。*/
                    ssn_close_nolock(zs, s->sid);
                }
                i++;
            }
            /* ハッシュテーブルのリストを解放します。*/
            hash_list_free((void**)s_list);
        }
        CS_END(&zs->critical_section);
    }
#ifdef _WIN32
    _endthread();
#endif
}

/*
 * セッション管理の初期処理を行ないます。
 *
 * 最大セッション数を指定しない場合は SESSION_UNLIMITED を指定します。
 * セッションのタイムアウトを指定しない場合は SESSION_NOTIMEOUT を指定します。
 * cbhost が NULL かホストのIPアドレスがゼロの場合はセッション識別IDに
 * ホスト名は含まれません。
 *
 * zname: ゾーン名
 * max_session: 最大セッション数
 * timeout: タイムアウト時間を秒で指定
 * cbhost: ホスト名を取得する関数のポインタ
 * cbcopy: セッションをコピーするサーバーの情報を取得する関数のポインタ
 *
 * 戻り値
 *  成功した場合はセッション管理構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct zone_session_t* ssn_initialize(const char* zname,
                                                int max_session,
                                                int timeout,
                                                struct srelay_server_t* rs)
{
    struct zone_session_t* zs;
    int capacity;
    time_t timebuf;
    struct tm now; 

    zs = (struct zone_session_t*)calloc(1, sizeof(struct zone_session_t));
    if (zs == NULL) {
        err_write("session: no memory.");
        return NULL;
    }

    strcpy(zs->zone_name, zname);
    zs->max_session = max_session;
    zs->cur_session = 0;
    zs->timeout = timeout;
    zs->rsvr = rs;

    capacity = (max_session > 0)? max_session : 100;

    zs->s_tbl = hash_initialize(capacity);
    if (zs->s_tbl == NULL) {
        err_write("session: no memory.");
        free(zs);
        return NULL;
    }

    /* 現在時刻の取得 */
    time(&timebuf);
    mt_localtime(&timebuf, &now);

    /* 乱数の種を設定 */
    srand(now.tm_sec);

    /* クリティカルセクションの初期化 */
    CS_INIT(&zs->critical_section);

    if (zs->timeout > 0) {
        /* タイムアウトを監視するスレッドを作成します。*/
#ifdef _WIN32
        uintptr_t timeout_thread_id;
        timeout_thread_id = _beginthread(session_timeout_thread, 0, zs);
#else
        pthread_t timeout_thread_id;
        pthread_create(&timeout_thread_id, NULL, (void*)session_timeout_thread, zs);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(timeout_thread_id);
#endif
    }
    return zs;
}

/*
 * セッション管理を終了します。
 *
 * zs: セッション管理構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_finalize(struct zone_session_t* zs)
{
    char** skey_list;

    if (zs == NULL)
        return;

    /* すべてのセッションをクローズ(2009/10/15) */
    CS_START(&zs->critical_section);
    skey_list = hash_keylist(zs->s_tbl);
    if (skey_list != NULL) {
        int i = 0;

        while (skey_list[i]) {
            struct session_t* s;

            s = (struct session_t*)hash_get(zs->s_tbl, skey_list[i]);
            if (s)
                ssn_close_nolock(zs, s->sid);
            i++;
        }
        hash_list_free((void**)skey_list);
    }

    /* セッションの解放 */
    hash_finalize(zs->s_tbl);
    CS_END(&zs->critical_section);

    /* クリティカルセクションの削除 */
    CS_DELETE(&zs->critical_section);

    if (zs->timeout > 0) {
        /* ssn_finalize()が呼ばれるのはプログラム終了時ですが、
           タイムアウトを監視するスレッドを終了するようにフラグを立てます。*/
        zs->thread_end_flag = 1;
    }
    free(zs);
}

/*
 * 新たなセッションを新規に作成します。
 *
 * req: リクエスト構造体のポインタ
 *
 * 戻り値
 *  セッション構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct session_t* ssn_create(struct request_t* req)
{
    struct session_t* s = NULL;
    struct zone_session_t* zs;
    char skey[SESSION_KEY_SIZE+1];
    char sid[MAX_SESSIONID];

    zs = req->zone;
    CS_START(&zs->critical_section);
    s = new_session(zs);
    if (s == NULL)
        goto final;

    /* セッション管理テーブルのキーを作成します。*/
    create_session_key(skey, zs, s);

    /* セッション管理テーブルにセッションを登録します。*/
    if (hash_put(zs->s_tbl, skey, s) < 0) {
        free(s);
        goto final;
    }
    zs->cur_session++;

    /* セッション識別子 sid の生成 */
    create_session_id(sid, skey, zs);

    strcpy(s->sid, sid);
    strcpy(s->skey, skey);
    if (zs->rsvr) {
        s->owner_addr = zs->rsvr->host_addr;
        s->owner_port = zs->rsvr->host_port;
        memcpy(&s->owner_s_cp, &zs->rsvr->s_cp, sizeof(struct session_copy_t));
    }
    s->owner_flag = 1;

final:
    CS_END(&zs->critical_section);
    req->session = s;
    return s;
}

/*
 * セッションキーとセッション識別子から新たなセッションを
 * 新規に作成します。
 *
 * zs: セッション管理構造体のポインタ
 * skey: セッションキー
 * sid: セッション識別子
 *
 * 戻り値
 *  セッション構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct session_t* ssn_copy_create(struct zone_session_t* zs,
                                            const char* skey,
                                            const char* sid)
{
    struct session_t* s = NULL;

    CS_START(&zs->critical_section);
    s = copy_session_create(zs, sid, skey);
    CS_END(&zs->critical_section);
    return s;
}

/*
 * セッション管理から該当のセッション構造体を検索します。
 *
 * セッション・リレー機能が設定されている場合はセッションの取得を行ないます。
 *
 * zs: セッション管理構造体のポインタ
 * sid: セッション識別子
 *
 * 戻り値
 *  セッション構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct session_t* ssn_target(struct zone_session_t* zs, const char* sid)
{
    struct session_t* s = NULL;
    char skey[SESSION_KEY_SIZE+1];

    CS_START(&zs->critical_section);
    if (sid_to_skey(zs, sid, skey) == 0) {
        s = (struct session_t*)hash_get(zs->s_tbl, skey);

        if (zs->rsvr) {
            /* セッション・リレーの場合にオーナー権を取得します。*/
            if (s == NULL) {
                /* セッションがない場合は内容を継承したセッションが作成されます。*/
                s = get_session_owner(zs, sid, skey, s);
            } else {
                if (! s->owner_flag)
                    s = get_session_owner(zs, sid, skey, s);
            }
        }
        if (s)
            s->last_access = system_time();
    }
    if (s)
        ssn_attach(s);
    CS_END(&zs->critical_section);
    return s;
}

/* 自分が生成したセッションのみセッションリレーで削除します。*/
static void session_remove_relay(struct zone_session_t* zs, struct session_t* s)
{
    if (is_own_session(zs, s->sid)) {
        char zname[MAX_ZONENAME];
        char hostname[MAX_HOSTNAME];
        int i;

        /* sidからゾーン名を取得します。*/
        sid_to_zone(zs, s->sid, zname);

        /* 自分が生成したセッションの場合 */
        if (! s->owner_flag) {
            /* 現在のオーナーのホスト名（IPアドレス）を取得します。*/
            mt_inet_addr(s->owner_addr, hostname);

            /* 現在のオーナーに削除コマンド発行します。*/
            srelay_delete_session(s->skey, zname, hostname, s->owner_port);
        }

        /* オーナーのコピーを持つサーバーに削除コマンドを発行します。
           自分がオーナーの場合は自分のコピーを持つサーバーに削除コマンドを発行します。*/
        for (i = 0; i < s->owner_s_cp.count; i++) {
            mt_inet_addr(s->owner_s_cp.addr[i], hostname);
            srelay_delete_session(s->skey, zname, hostname, s->owner_s_cp.port[i]);
        }
    }
}

/*
 * セッション構造体が確保したメモリを解放します。
 * セッションのデータはすべて削除されます。
 *
 * この関数はセッションを非ブロック状態で処理します。
 *
 * s: セッション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_free_nolock(struct session_t* s)
{
    if (s->sdata) {
        ssn_delete_all_nolock(s);
        hash_finalize(s->sdata);
    }
    free(s);
}

/*
 * セッションの使用を終了します。
 * セッション管理から該当のセッション構造体が削除されます。
 *
 * zs: セッション管理構造体のポインタ
 * sid: セッション識別子
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_close(struct zone_session_t* zs, const char* sid)
{
    CS_START(&zs->critical_section);
    ssn_close_nolock(zs, sid);
    CS_END(&zs->critical_section);
}

/*
 * セッションの使用を終了します。
 * セッション管理から該当のセッション構造体が削除されます。
 *
 * この関数はセッションを非ブロック状態で処理します。
 *
 * zs: セッション管理構造体のポインタ
 * sid: セッション識別子
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_close_nolock(struct zone_session_t* zs, const char* sid)
{
    char skey[SESSION_KEY_SIZE+1];

    if (sid_to_skey(zs, sid, skey) == 0) {
        struct session_t* s;

        s = (struct session_t*)hash_get(zs->s_tbl, skey);
        if (s) {
            if (zs->rsvr) {
                /* セッション・リレーで削除します。*/
                session_remove_relay(zs, s);
            }
            ssn_free_nolock(s);
        }
        /* セッション管理から削除します。*/
        hash_delete(zs->s_tbl, skey);
    }
}

/*
 * セッションからキー値の値を取得します。
 *
 * s: セッション構造体のポインタ
 * key: キー値のポインタ
 *
 * 戻り値
 *  値のポインタを返します。
 *  キー値が存在しない場合は NULL を返します。
 */
APIEXPORT void* ssn_get(struct session_t* s, const char* key)
{
    struct session_data_t* sd;

    s->last_access = system_time();
    sd = (struct session_data_t*)hash_get(s->sdata, key);
    if (sd == NULL)
        return NULL;
    return sd->data;
}

/*
 * セッションにキー値と文字列のペアを登録します。
 *
 * キー値がすでに登録されている場合は置き換えられます。
 *
 * 文字列のコピーがセッションに登録されますので
 * スタック上の領域をセッションに登録することも可能です。
 *
 * s: セッション構造体のポインタ
 * key: キー値のポインタ
 * str: 文字列のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_put(struct session_t* s, const char* key, const char* str)
{
    ssn_putdata(s, key, str, (int)strlen(str)+1);
}

/*
 * セッションにキー値とデータのペアを登録します。
 *
 * キー値がすでに登録されている場合は置き換えられます。
 *
 * データのコピーがセッションに登録されますので
 * スタック上の領域をセッションに登録することも可能です。
 * データのバイト数にゼロを指定した場合は、データのポインタのみセッションに設定されます。
 * この場合のデータのポインタは、グローバルな領域である必要があります。
 *
 * s: セッション構造体のポインタ
 * key: キー値のポインタ
 * data: データのポインタ
 * size: データのバイト数
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_putdata(struct session_t* s, const char* key, const void* data, int size)
{
    CS_START(&s->zs->critical_section);
    ssn_put_nolock(s, key, data, size);
    CS_END(&s->zs->critical_section);
}

/*
 * セッションにキー値とデータのペアを登録します。
 *
 * この関数はセッションを非ブロック状態で登録します。
 * セッション・リレーで使用されます。
 * 通常はブロック状態で作用するssn_putdata()を使用します。
 *
 * キー値がすでに登録されている場合は置き換えられます。
 *
 * データのコピーがセッションに登録されますので
 * スタック上の領域をセッションに登録することも可能です。
 * データのバイト数にゼロを指定した場合は、データのポインタのみセッションに設定されます。
 * この場合のデータのポインタは、グローバルな領域である必要があります。
 *
 * s: セッション構造体のポインタ
 * key: キー値のポインタ
 * data: データのポインタ
 * size: データのバイト数
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_put_nolock(struct session_t* s, const char* key, const void* data, int size)
{
    struct session_data_t* sd;

    /* すでにキー値が存在している場合はセッションデータ構造体を解放します。*/
    sd = (struct session_data_t*)hash_get(s->sdata, key);
    if (sd != NULL)
        free(sd);

    /* セッションデータ構造体とデータ領域を連続した領域で確保します。*/
    sd = (struct session_data_t*)malloc(sizeof(struct session_data_t) + size);
    if (sd == NULL) {
        err_write("session: ssn_putdata() no memory.");
        return;
    }

    sd->size = size;
    if (size == 0) {
        /* データ領域は確保せずにポインタだけを設定します。*/
        sd->data = (void*)data;
    } else {
        sd->data = sd + 1;
        memcpy(sd->data, data, size);
    }

    s->last_access = s->last_update = system_time();
    hash_put(s->sdata, key, sd);
}

/*
 * セッションからキー値に該当するデータを削除します。
 *
 * s: セッション構造体のポインタ
 * key: キー値のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_delete(struct session_t* s, const char* key)
{
    struct session_data_t* sd;

    CS_START(&s->zs->critical_section);

    /* キー値のセッションデータ構造体を解放します。*/
    sd = (struct session_data_t*)hash_get(s->sdata, key);
    if (sd != NULL)
        free(sd);

    hash_delete(s->sdata, key);
    s->last_access = s->last_update = system_time();

    CS_END(&s->zs->critical_section);
}

/*
 * セッションデータをすべて削除します。
 * セッションデータはゼロ件になります。
 *
 * s: セッション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_delete_all(struct session_t* s)
{
    CS_START(&s->zs->critical_section);
    ssn_delete_all_nolock(s);
    CS_END(&s->zs->critical_section);
}

/*
 * セッションデータをすべて削除します。
 * セッションデータはゼロ件になります。
 *
 * この関数はセッションを非ブロック状態で削除します。
 *
 * s: セッション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_delete_all_nolock(struct session_t* s)
{
    if (s->sdata) {
        char** keys;

        keys = hash_keylist(s->sdata);
        if (keys) {
            int i = 0;

            while (keys[i]) {
                struct session_data_t* sd;

                sd = (struct session_data_t*)hash_get(s->sdata, keys[i]);
                if (sd)
                    free(sd);
                hash_delete(s->sdata, keys[i]);
                i++;
            }
            hash_list_free((void**)keys);
        }
    }
    s->last_access = s->last_update = system_time();
}

/*
 * リクエストにセッションが結びつけられる時点で本関数が呼ばれます。
 * ssn_target()から呼び出されます。
 *
 * s: セッション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_attach(struct session_t* s)
{
    if (s == NULL)
        return;

    s->attach_last_update = s->last_update;
}

/*
 * リクエストが解放される時点で本関数が呼ばれます。
 * セッションを切り離す際の処理を記述します。
 *
 * セッションコピーが行われます。
 *
 * s: セッション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void ssn_detach(struct session_t* s)
{
    if (s == NULL)
        return;

    if (s->last_update != s->attach_last_update) {
        struct zone_session_t* zs;

        /* セッションが更新されています。*/
        zs = s->zs;
        if (zs->rsvr) {
            int i;

            /* セッションリレーが有効で
               自分のセッションのコピーを持つサーバーがある場合はコピーします。*/
            for (i = 0; i < zs->rsvr->s_cp.count; i++) {
                char cp_host[MAX_HOSTNAME];
                char owner_host[MAX_HOSTNAME];

                mt_inet_addr(zs->rsvr->s_cp.addr[i], cp_host);
                mt_inet_addr(s->owner_addr, owner_host);

                srelay_copy_session(s, 
                                    s->skey,
                                    zs->zone_name,
                                    cp_host,
                                    zs->rsvr->s_cp.port[i],
                                    owner_host,
                                    s->owner_port,
                                    &s->owner_s_cp);
            }
        }
    }
}
