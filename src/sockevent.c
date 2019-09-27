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

#ifdef HAVE_EPOLL
#include <sys/epoll.h>
#elif defined(HAVE_KQUEUE)
#include <sys/event.h>
#endif
#define MAX_EVENTS 10

#define API_INTERNAL
#include "nestalib.h"

struct sock_event_t {
#ifdef HAVE_EPOLL /* Linux epoll() */
    int epfd;
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    int kqfd;
#else /* POSIX select() */
    CS_DEF(critical_section);
    fd_set rd;
    SOCKET max_socket;
    struct hash_t* ht;         /* socket hash table
                                *   key  : htkey
                                *   data : strcut sock_data_t*
                                */
#endif
};

#if !defined(HAVE_EPOLL) && !defined(HAVE_KQUEUE)
/* use select(2) only */
#define HASHKEY_SIZE 16

struct sock_data_t {
    char htkey[HASHKEY_SIZE];       /* hash key */
    SOCKET socket;                  /* socket */
    int disable;                    /* disable flag */
};
#endif


#ifdef HAVE_EPOLL
/* Linux epoll() を使用してイベントを待ちます。*/
static void do_epoll(int sc,
                     const SOCKET sockets[],
                     const SOCK_EVENT_CB cbfuncs[],
                     const SOCK_EVENT_BREAK_CB breakfunc)
{
    int epfd;
    struct epoll_event ev;
    int i;

    epfd = epoll_create(sc);
    if (epfd < 0) {
        err_write("epoll_create() failed");
        return;
    }

    for (i = 0; i < sc; i++) {
        memset(&ev, 0, sizeof(ev));
        ev.events  = EPOLLIN;
        ev.data.fd = sockets[i];

        if (epoll_ctl(epfd, EPOLL_CTL_ADD, sockets[i], &ev) < 0) {
            err_write("epoll_ctl() failed");
            close(epfd);
            return;
        }
    }

    while (! (*breakfunc)()) {
        int n;
        struct epoll_event evs[MAX_EVENTS];

        n = epoll_wait(epfd, evs, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR || errno == 0)
                continue;
            err_write("epoll_wait() failed: %s", strerror(errno));
            break;
        } else if (n > 0) {
            /* クライアントからの接続要求があった場合 */
            for (i = 0; i < n; i++) {
                int j;

                for (j = 0; j < sc; j++) {
                    if (evs[i].data.fd == sockets[j]) {
                        /* イベント関数の呼び出し */
                        if ((*cbfuncs[j])(sockets[j]) < 0)
                            break;
                    }
                }
            }
        }
    }
    close(epfd);
}
#elif defined(HAVE_KQUEUE)
/* BSD kqueue() を使用してイベントを待ちます。*/
static void do_kqueue(int sc,
                      const SOCKET sockets[],
                      const SOCK_EVENT_CB cbfuncs[],
                      const SOCK_EVENT_BREAK_CB breakfunc)
{
    int kqfd;
    struct kevent kev;
    int i;

    kqfd = kqueue();
    if (kqfd < 0) {
        err_write("kqueue() failed");
        return;
    }

    for (i = 0; i < sc; i++) {
        EV_SET(&kev, sockets[i], EVFILT_READ, EV_ADD, 0, 0, NULL);
        if (kevent(kqfd, &kev, 1, NULL, 0, NULL) < 0) {
            err_write("kevent() failed");
            return;
        }
    }

    while (! (*breakfunc)()) {
        struct kevent kevs[MAX_EVENTS];
        int n;

        n = kevent(kqfd, NULL, 0, kevs, MAX_EVENTS, NULL);
        if (n < 0) {
            if (errno == EINTR || errno == 0)
                continue;
            err_write("Failed to kevent socket: %s", strerror(errno));
            break;
        } else if (n > 0) {
            for (i = 0; i < n; i++) {
                int j;

                /* クライアントからの接続要求があった場合 */
                for (j = 0; j < sc; j++) {
                    if (kevs[i].ident == sockets[j]) {
                        /* イベント関数の呼び出し */
                        if ((*cbfuncs[j])(sockets[j]) < 0)
                            break;
                    }
                }
            }
        }
    }
    close(kqfd);
}
#else /* POSIX select() */
static void do_select(int sc,
                      const SOCKET sockets[],
                      const SOCK_EVENT_CB cbfuncs[],
                      const SOCK_EVENT_BREAK_CB breakfunc)
{
    int i;
    SOCKET max_socket = 0;

    /* select()のためにソケットの最大値を求めます。*/
    for (i = 0; i < sc; i++) {
        if (sockets[i] > max_socket)
            max_socket = sockets[i];
    }

    while (! (*breakfunc)()) {
        fd_set rd;

        FD_ZERO(&rd);
        for (i = 0 ; i < sc; i++)
            FD_SET(sockets[i], &rd);

        /* クライアントからの接続を待機します。*/
        if (select(max_socket+1, &rd, NULL, NULL, NULL) < 0) {
            /* エラーの詳細を取得し、シグナルが発生したのであれば無視します。*/
            if (errno == EINTR || errno == 0)
                continue;
            err_write("Failed to select server socket: %s", strerror(errno));
            break;
        }

        /* クライアントからの接続要求があった場合 */
        for (i = 0; i < sc; i++) {
            if (FD_ISSET(sockets[i], &rd)) {
                /* イベント関数の呼び出し */
                if ((*cbfuncs[i])(sockets[i]) < 0)
                    break;
            }
        }
    }
}
#endif

/*
 * socketイベントを登録します。
 * 該当のイベントが発生した場合はイベント関数を呼び出します。
 *
 * sc: ソケット登録数
 * sockets: ソケット配列
 * cbfunc: イベント関数のポインタ配列
 * breakfunc: イベントループ終了判定する関数のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void sock_event(int sc,
                          const SOCKET sockets[],
                          const SOCK_EVENT_CB cbfuncs[],
                          const SOCK_EVENT_BREAK_CB breakfunc)
{
#ifdef HAVE_EPOLL /* Linux epoll() */
    do_epoll(sc, sockets, cbfuncs, breakfunc);
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    do_kqueue(sc, sockets, cbfuncs, breakfunc);
#else /* POSIX select() */
    do_select(sc, sockets, cbfuncs, breakfunc);
#endif
}

#if !defined(HAVE_EPOLL) && !defined(HAVE_KQUEUE)
static char* sock_hashkey(SOCKET socket, char* keybuf, int bufsize)
{
    snprintf(keybuf, bufsize, "%d", socket);
    return keybuf;
}

static struct sock_data_t* sock_data_alloc(SOCKET socket, const char* htkey)
{
    struct sock_data_t* sd;

    sd = (struct sock_data_t*)calloc(1, sizeof(struct sock_data_t));
    if (sd == NULL) {
        err_write("sock_data_alloc: no memory");
        return NULL;
    }
    strcpy(sd->htkey, htkey);
    sd->socket = socket;
    sd->disable = 0;
    return sd;
}

static void sock_data_free(struct sock_data_t* sd)
{
    if (sd)
        free(sd);
}

static void sock_list_free(struct sock_event_t* seve)
{
    struct sock_data_t** sc_list;

    /* 管理しているソケット領域を解放します。*/
    sc_list = (struct sock_data_t**)hash_list(seve->ht);
    if (sc_list) {
        int i = 0;

        while (sc_list[i]) {
            sock_data_free(sc_list[i]);
            i++;
        }
        hash_list_free((void**)sc_list);
    }
    hash_finalize(seve->ht);
}
#endif

/*
 * socketイベントの使用を開始します。
 *
 * 戻り値
 *  socketイベント識別子のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT void* sock_event_create()
{
    struct sock_event_t* seve;

    seve = (struct sock_event_t*)calloc(1, sizeof(struct sock_event_t));
    if (seve == NULL) {
        err_write("sock_event_create() no memory");
        return NULL;
    }

#ifdef HAVE_EPOLL /* Linux epoll() */
    seve->epfd = epoll_create(MAX_EVENTS);
    if (seve->epfd < 0) {
        free(seve);
        err_write("epoll_create() failed");
        return NULL;
    }
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    seve->kqfd = kqueue();
    if (seve->kqfd < 0) {
        free(seve);
        err_write("kqueue() failed");
        return NULL;
    }
#else /* POSIX select() */
    FD_ZERO(&seve->rd);
    CS_INIT(&seve->critical_section);

    seve->ht = hash_initialize(1031);
    if (seve->ht == NULL) {
        free(seve);
        err_write("sock_event_create(): hash_initialize() failed");
        return NULL;
    }
#endif
    return seve;
}

/*
 * socketイベントを登録します。
 *
 * seve: ソケットイベント識別子のポインタ
 * socket: ソケット
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sock_event_add(const void* sev, SOCKET socket)
{
    struct sock_event_t* seve;

#ifdef HAVE_EPOLL /* Linux epoll() */
    struct epoll_event ev;

    seve = (struct sock_event_t*)sev;
    memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = socket;

    if (epoll_ctl(seve->epfd, EPOLL_CTL_ADD, socket, &ev) < 0) {
        err_write("epoll_ctl(add) failed");
        return -1;
    }
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    struct kevent kev;

    seve = (struct sock_event_t*)sev;
    EV_SET(&kev, socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
    if (kevent(seve->kqfd, &kev, 1, NULL, 0, NULL) < 0) {
        err_write("kevent(add) failed");
        return -1;
    }
#else /* POSIX select() */
    char htkey[HASHKEY_SIZE];
    struct sock_data_t* sd;

    seve = (struct sock_event_t*)sev;
    CS_START(&seve->critical_section);
    FD_SET(socket, &seve->rd);
    sock_hashkey(socket, htkey, sizeof(htkey));
    sd = sock_data_alloc(socket, htkey);
    if (sd == NULL) {
        err_write("sock_event_add(): no memory");
        CS_END(&seve->critical_section);
        return -1;
    }
    hash_put(seve->ht, htkey, sd);
    /* select()のためにソケットの最大値を求めます。*/
    if (socket > seve->max_socket)
        seve->max_socket = socket;
    CS_END(&seve->critical_section);
#endif
    return 0;
}

/*
 * socketイベントを削除します。
 *
 * seve: ソケットイベント識別子のポインタ
 * socket: ソケット
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sock_event_delete(const void* sev, SOCKET socket)
{
    struct sock_event_t* seve;

#ifdef HAVE_EPOLL /* Linux epoll() */
    seve = (struct sock_event_t*)sev;

    if (epoll_ctl(seve->epfd, EPOLL_CTL_DEL, socket, NULL) < 0) {
        err_write("epoll_ctl(delete) failed");
        return -1;
    }
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    struct kevent kev;

    seve = (struct sock_event_t*)sev;
    EV_SET(&kev, socket, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    if (kevent(seve->kqfd, &kev, 1, NULL, 0, NULL) < 0) {
        err_write("kevent(delete) failed");
        return -1;
    }
#else /* POSIX select() */
    char htkey[HASHKEY_SIZE];
    struct sock_data_t* sd;

    seve = (struct sock_event_t*)sev;
    CS_START(&seve->critical_section);
    FD_CLR(socket, &seve->rd);
    sock_hashkey(socket, htkey, sizeof(htkey));
    sd = hash_get(seve->ht, htkey);
    if (sd)
        sock_data_free(sd);
    hash_delete(seve->ht, htkey);

    /* select()のためにソケットの最大値を求めます。*/
    if (socket == seve->max_socket) {
        int i = 0;
        struct sock_data_t** sc_list;

        sc_list = (struct sock_data_t**)hash_list(seve->ht);
        if (sc_list == NULL) {
            err_write("sock_event_delete: hash_list() failed.");
            CS_END(&seve->critical_section);
            return -1;
        }

        seve->max_socket = 0;
        while (sc_list[i]) {
            if (sc_list[i]->socket > seve->max_socket)
                seve->max_socket = sc_list[i]->socket;
            i++;
        }
        hash_list_free((void**)sc_list);
    }
    CS_END(&seve->critical_section);
#endif
    return 0;
}

/*
 * socketイベントの通知を無効にします。
 *
 * seve: ソケットイベント識別子のポインタ
 * socket: ソケット
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sock_event_disable(const void* sev, SOCKET socket)
{
    struct sock_event_t* seve;

#ifdef HAVE_EPOLL /* Linux epoll() */
    struct epoll_event ev;

    seve = (struct sock_event_t*)sev;
    memset(&ev, 0, sizeof(ev));
    ev.events  = 0;
    ev.data.fd = socket;

    if (epoll_ctl(seve->epfd, EPOLL_CTL_MOD, socket, &ev) < 0) {
        err_write("epoll_ctl(disable) failed");
        return -1;
    }
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    struct kevent kev;

    seve = (struct sock_event_t*)sev;
    EV_SET(&kev, socket, EVFILT_READ, EV_DISABLE, 0, 0, NULL);
    if (kevent(seve->kqfd, &kev, 1, NULL, 0, NULL) < 0) {
        err_write("kevent(disable) failed");
        return -1;
    }
#else /* POSIX select() */
    char htkey[HASHKEY_SIZE];
    struct sock_data_t* sd;

    seve = (struct sock_event_t*)sev;
    CS_START(&seve->critical_section);
    sock_hashkey(socket, htkey, sizeof(htkey));
    sd = hash_get(seve->ht, htkey);
    if (sd)
        sd->disable = 1;
    CS_END(&seve->critical_section);
#endif
    return 0;
}

/*
 * socketイベントの通知を有効にします。
 *
 * seve: ソケットイベント識別子のポインタ
 * socket: ソケット
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int sock_event_enable(const void* sev, SOCKET socket)
{
    struct sock_event_t* seve;

#ifdef HAVE_EPOLL /* Linux epoll() */
    struct epoll_event ev;

    seve = (struct sock_event_t*)sev;
    memset(&ev, 0, sizeof(ev));
    ev.events  = EPOLLIN;
    ev.data.fd = socket;

    if (epoll_ctl(seve->epfd, EPOLL_CTL_MOD, socket, &ev) < 0) {
        err_write("epoll_ctl(enable) failed");
        return -1;
    }
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    struct kevent kev;

    seve = (struct sock_event_t*)sev;
    EV_SET(&kev, socket, EVFILT_READ, EV_ENABLE, 0, 0, NULL);
    if (kevent(seve->kqfd, &kev, 1, NULL, 0, NULL) < 0) {
        err_write("kevent(enable) failed");
        return -1;
    }
#else /* POSIX select() */
    char htkey[HASHKEY_SIZE];
    struct sock_data_t* sd;

    seve = (struct sock_event_t*)sev;
    CS_START(&seve->critical_section);
    sock_hashkey(socket, htkey, sizeof(htkey));
    sd = hash_get(seve->ht, htkey);
    if (sd)
        sd->disable = 0;
    CS_END(&seve->critical_section);
#endif
    return 0;
}

#if !defined(HAVE_EPOLL) && !defined(HAVE_KQUEUE)
static struct membuf_t* select_key_list(struct sock_event_t* seve)
{
    int i = 0;
    char** key_list;
    struct membuf_t* mb;

    key_list = hash_keylist(seve->ht);
    if (key_list == NULL) {
        err_write("select_key_list: hash_keylist() failed.");
        return NULL;
    }
    mb = mb_alloc(1024);
    if (mb == NULL) {
        hash_list_free((void**)key_list);
        err_write("select_key_list: no memory.");
        return NULL;
    }
    while (key_list[i]) {
        if (mb_append(mb, key_list[i], (int)strlen(key_list[i]) + 1) < 0) {
            mb_free(mb);
            hash_list_free((void**)key_list);
            err_write("select_key_list: no memory.");
            return NULL;
        }
        i++;
    }
    mb_append(mb, "\0", 1);
    hash_list_free((void**)key_list);
    return mb;
}
#endif

/*
 * イベント処理を開始します。
 * 該当のイベントが発生した場合はイベント関数を呼び出します。
 *
 * seve: ソケットイベント識別子のポインタ
 * cbfunc: イベント関数のポインタ
 * breakfunc: イベントループ終了判定する関数のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void sock_event_loop(const void* sev,
                               const SOCK_EVENT_CB cbfunc,
                               const SOCK_EVENT_BREAK_CB breakfunc)
{
    struct sock_event_t* seve;

    seve = (struct sock_event_t*)sev;
#ifdef HAVE_EPOLL /* Linux epoll() */
    while (! (*breakfunc)()) {
        int n;
        struct epoll_event evs[MAX_EVENTS];

        n = epoll_wait(seve->epfd, evs, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR || errno == 0)
                continue;
            err_write("epoll_wait() failed: %s", strerror(errno));
            break;
        } else if (n > 0) {
            int i;

            /* クライアントからの接続要求があった場合 */
            for (i = 0; i < n; i++) {
                /* イベント関数の呼び出し */
                if ((*cbfunc)(evs[i].data.fd) < 0)
                    break;
            }
        }
    }
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    while (! (*breakfunc)()) {
        struct kevent kevs[MAX_EVENTS];
        int n;

        n = kevent(seve->kqfd, NULL, 0, kevs, MAX_EVENTS, NULL);
        if (n < 0) {
            if (errno == EINTR || errno == 0)
                continue;
            err_write("Failed to kevent socket: %s", strerror(errno));
            break;
        } else if (n > 0) {
            int i;

            for (i = 0; i < n; i++) {
                /* イベント関数の呼び出し */
                if ((*cbfunc)(kevs[i].ident) < 0)
                    break;
            }
        }
    }
#else /* POSIX select() */
    while (! (*breakfunc)()) {
        fd_set sel_rd;
        struct membuf_t* mb;
        char* key;

        /* select(2) は渡された fd_set 構造体を書き換えてしまうのでコピーを渡します。*/
        memcpy(&sel_rd, &seve->rd, sizeof(sel_rd));

        /* クライアントからの接続を待機します。*/
        if (select(seve->max_socket+1, &sel_rd, NULL, NULL, NULL) < 0) {
            /* エラーの詳細を取得し、シグナルが発生したのであれば無視します。*/
            if (errno == EINTR || errno == 0)
                continue;
            err_write("Failed to select server socket: %s", strerror(errno));
            break;
        }

        /* キーのリスト（コピー）を作成します。
           クリティカルセクションを抜けたあとにセッションが削除される可能性があるため
           キーのコピーを取得します。*/
        CS_START(&seve->critical_section);
        mb = select_key_list(seve);
        if (mb == NULL) {
            CS_END(&seve->critical_section);
            return;
        }
        CS_END(&seve->critical_section);

        /* 管理しているソケットにイベントが発生したか調べます。*/
        key = mb->buf;
        while (*key) {
            struct sock_data_t* sc;

            sc = (struct sock_data_t*)hash_get(seve->ht, key);
            if (sc) {
                if (FD_ISSET(sc->socket, &sel_rd)) {
                    if (! sc->disable) {
                        /* イベント関数の呼び出し */
                        if ((*cbfunc)(sc->socket) < 0)
                            break;
                    }
                }
            }
            key += strlen(key) + 1;
        }
        mb_free(mb);
    }
#endif
}

/*
 * イベント処理を終了します。
 *
 * seve: ソケットイベント識別子のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void sock_event_close(const void* sev)
{
    struct sock_event_t* seve;

    seve = (struct sock_event_t*)sev;
    if (seve == NULL)
        return;

#ifdef HAVE_EPOLL /* Linux epoll() */
    close(seve->epfd);
#elif defined(HAVE_KQUEUE) /* BSD kqueue(), kevent() */
    close(seve->kqfd);
#else /* POSIX select() */
    /* クリティカルセクションの削除 */
    CS_DELETE(&seve->critical_section);
    sock_list_free(seve);
#endif

    free(seve);
}
