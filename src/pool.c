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
#include "pool.h"

/*
 * データベースへのコネクションなどを再利用するための関数群です。
 * スレッドセーフで動作します。
 *
 *【使用例】
 * 1. プーリングの初期化を行ないます。
 *      pool = pool_initialize(初期にプールするデータの個数,
 *                             拡張するデータの個数,
 *                             プールするデータを取得するコールバック関数,
 *                             データを解放するコールバック関数,
 *                             タイムアウト時間（ミリ秒）,
 *                             拡張したデータの解放待ち時間（秒）,
 *                             固有データ)
 * 2. 初期にプールする回数分コールバック関数が呼び出されます。
 *    呼び出し時に固有データがパラメータとして渡されます。
 *    コネクションを取得して関数値として返します。
 * 3. アプリケーションはコネクションを取得します。
 *      con = pool_get(pool, POOL_NOWAIT);
 * 5. コネクションを利用した処理を行ないます。
 * 6. コネクションをリリースします。
 *      pool_release(pool, con);
 * 7. 終了処理を行ないます。
 *      pool_finalize(pool);
 *    データを解放するコールバック関数が呼び出されます。
 */

/* 拡張したデータをプールから解放するスレッド */
static void pool_release_thread(void* argv)
{
    struct pool_t* p;
    int stime;

    p = (struct pool_t*)argv;
    if (p->release_time <= 0)
        return;

    /* 指定されたタイムアウト（秒）の1/2の間スリープします。*/
    stime = p->release_time / 2;
#ifdef _WIN32
    /* windowsのSleep()はミリ秒になります。*/
    stime *= 1000;
#endif

    while (1) {
        struct pool_element_t* e;
        int64 now_time;
        int i;
        int num;

        /* 指定時間眠りに入りますzzz。*/
        sleep(stime);

        /* pool_finalize()が呼ばれていたらスレッドを終了します。*/
        if (p->end_flag)
            break;

        /* 解放するデータがあるか調べてコールバック関数を呼び出します。*/
        CS_START(&p->critical_section);
        now_time = system_time();

        e = p->e + (p->element_num - 1);
        for (i = p->element_num-1; i >= p->init_num; i--) {
            int release_flag = 0;

            if (e->used) {
                /* タイムアウトを判定 */
                if (p->timeout_ms > 0) {
                    long elap;

                    elap = (long)((now_time - e->systime) / 1000L);
                    if (elap > p->timeout_ms)
                        release_flag = 1;
                }
            } else {
                /* 未使用時間（秒）を判定 */
                if (p->release_time > 0) {
                    int elap;

                    elap = (int)((now_time - e->last_access) / 1000000LL);
                    if (elap > p->release_time)
                        release_flag = 1;
                }
            }
            if (release_flag) {
                /* タイムアウトしているので解放します。*/
                if (p->cb_remove != NULL) {
                    /* コールバックの呼び出し */
                    (*p->cb_remove)(e->data);
                }
                if (i < p->element_num-1) {
                    int num;

                    /* shift */
                    num = p->element_num - (i + 1);
                    memmove(e, e+1, sizeof(struct pool_element_t) * num);
                }
                p->element_num--;
            }
            e--;
        }

        /* capacity area clear */
        e = p->e + p->element_num;
        num = p->capacity - p->element_num;
        if (num > 0)
            memset(e, '\0', sizeof(struct pool_element_t) * num);
        CS_END(&p->critical_section);
    }

#ifdef _WIN32
    _endthread();
#endif
}

/*
 * プーリングの初期処理を行ないます。
 * データを管理するためのメモリを確保します。
 *
 * この関数が実行されると init_num の回数分 cb_add() が呼び出されます。
 *
 * init_num: 初期にプールするデータの個数
 * extend_num: 拡張するデータの個数
 * cb_add: プールするデータを取得する関数
 * cb_remove: 拡張したデータを解放する場合に呼び出される関数(NULLも可)
 * timeout_ms: タイムアウト時間（ミリ秒）
 *             タイムアウトを指定しない場合は POOL_NOTIMEOUT
 * ext_release_time: 拡張したデータの解放待ち時間（秒）
 * param: コールバック関数に渡される固有データ(NULLも可)
 *
 * 戻り値
 *  プーリング構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct pool_t* pool_initialize(int init_num,
                                         int extend_num,
                                         CALLBACK_POOL_ADD cb_add,
                                         CALLBACK_POOL_REMOVE cb_remove,
                                         long timeout_ms,
                                         int ext_release_time,
                                         const void* param)
{
    struct pool_t* p;
    struct pool_element_t* e;
    int capacity;
    int i;

    if (cb_add == NULL) {
        err_write("pool: CALLBACK_POOL_ADD function is NULL.");
        return NULL;
    }
    p = (struct pool_t*)malloc(sizeof(struct pool_t));
    if (p == NULL) {
        err_write("pool: No memory.");
        return NULL;
    }
    capacity = init_num + extend_num;
    p->e = (struct pool_element_t*)calloc(capacity, sizeof(struct pool_element_t));
    if (p->e == NULL) {
        err_write("pool: No memory.");
        free(p);
        return NULL;
    }

    p->init_num = init_num;
    p->capacity = capacity;
    p->element_num = 0;
    p->timeout_ms = timeout_ms;
    p->cb_add = cb_add;
    p->cb_remove = cb_remove;
    p->release_time = ext_release_time;
    p->end_flag = 0;
    p->param = (void*)param;

    /* クリティカルセクションの初期化 */
    CS_INIT(&p->critical_section);

    /* 初期値分のデータをコールバックで取得します。*/
    e = p->e;
    for (i = 0; i < init_num; i++) {
        e->data = (*p->cb_add)((void*)param);
        if (e->data == NULL) {
            err_write("pool: cb_add() is NULL.");
            free(p->e);
            free(p);
            return NULL;
        }
        p->element_num++;
        e++;
    }

    if (extend_num > 0 && p->release_time > 0) {
        /* 拡張したデータを解放するスレッドを作成します。*/
#ifdef _WIN32
        uintptr_t release_thread_id;
        release_thread_id = _beginthread(pool_release_thread, 0, p);
#else
        pthread_t release_thread_id;
        pthread_create(&release_thread_id, NULL, (void*)pool_release_thread, p);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(release_thread_id);
#endif
    }
    return p;
}

/*
 * プーリングの終了処理を行ないます。
 * pool_initialize()で cb_remove が設定されている場合は呼び出されます。
 * データを管理するために使用されていたメモリは解放されます。
 *
 * pool: プーリング構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void pool_finalize(struct pool_t* p)
{
    if (p->cb_remove != NULL) {
        struct pool_element_t* e;
        int i;

        CS_START(&p->critical_section);
        e = p->e;
        for (i = 0; i < p->element_num; i++) {
            (*p->cb_remove)(e->data);
            e++;
        }
        p->capacity = 0;
        p->element_num = 0;
        p->end_flag = 1;
        CS_END(&p->critical_section);
    }

    /* クリティカルセクションの削除 */
    CS_DELETE(&p->critical_section);

    /* メモリの解放 */
    if (p) {
        if (p->e)
            free(p->e);
        free(p);
    }
}

/*
 * プーリングされているデータ数を返します。
 *
 * pool: プーリング構造体のポインタ
 *
 * 戻り値
 *  プーリングされているデータ数を返します。
 */
APIEXPORT int pool_count(struct pool_t* p)
{
    return p->element_num;
}

static void* get_data(struct pool_t* p)
{
    struct pool_element_t* e;
    int n;
    void* data = NULL;

    CS_START(&p->critical_section);
    e = p->e;
    n = p->element_num;
    while (n--) {
        if (! e->used) {
            if (e->data != NULL) {
                e->used = 1;
                e->systime = system_time();
                e->last_access = e->systime;
                data = e->data;
                break;
            }
        }
        e++;
    }
    CS_END(&p->critical_section);
    return data;
}

/*
 * プーリングされているデータから未使用のデータを取得します。
 *
 * すべてのデータが使用中の場合で拡張分に余裕がある場合は
 * コールバックにて拡張分のデータを取得して返します。
 * 拡張分も含めて使用中の場合は wait_time が指定されている場合に
 * 1秒おきに未使用データを検索します。wait_time 時間内にデータが
 * 取得できない場合は NULL が返されます。
 * 未使用データの取得チェックを行なわない場合は wait_time に
 * POOL_NOWAITを指定します。
 *
 * pool: プーリング構造体のポインタ
 * wait_time: 未使用データがない場合の最大待ち時間（秒）
 *
 * 戻り値
 *  データのポインタを返します。
 *  すべてが使用中の場合はNULLを返します。
 */
APIEXPORT void* pool_get(struct pool_t* p, int wait_time)
{
    void* data = NULL;

    /* 未使用データを取得します。*/
    data = get_data(p);

    if (data == NULL) {
        /* 拡張分のデータを取得します。*/
        CS_START(&p->critical_section);
        if (p->element_num < p->capacity) {
            struct pool_element_t* e;

            e = p->e;
            e += p->element_num;
            data = (*p->cb_add)(p->param);
            if (data != NULL) {
                e->data = data;
                e->used = 1;
                e->systime = system_time();
                e->last_access = e->systime;
                p->element_num++;
            }
        }
        CS_END(&p->critical_section);
    }

    if (data == NULL && wait_time > 0) {
        int timeout_flag = 0;
        int64 start_us;

        start_us = system_time();
        while (data == NULL && !timeout_flag) {
            int stime;

            /* 1秒間待ちます。*/
            stime = 1;
#ifdef _WIN32
            /* windowsのSleep()はミリ秒になります。*/
            stime *= 1000;
#endif
            sleep(stime);

            /* 未使用データを取得します。*/
            data = get_data(p);
            if (data == NULL) {
                int64 cur_us;

                /* 経過時間が指定時間を越えていたらタイムアウトとします。*/
                cur_us = system_time();
                if ((cur_us - start_us) / 1000000LL > wait_time)
                    timeout_flag = 1;
            }
        }
    }
    return data;
}


/*
 * 使用済みのデータをプーリングに返却します。
 * 返却されたデータは他で再利用できる状態になります。
 *
 * pool: プーリング構造体のポインタ
 * data: データのポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void pool_release(struct pool_t* p, void* data)
{
    struct pool_element_t* e;
    int n;

    CS_START(&p->critical_section);
    e = p->e;
    n = p->element_num;
    while (n--) {
        if (e->used) {
            if (e->data == data) {
                e->used = 0;
                e->systime = 0;
                e->last_access = system_time();
                break;
            }
        }
        e++;
    }
    CS_END(&p->critical_section);
}

/*
 * 使用済みのデータを再作成します。
 * プールされたデータが解放され、再度作成されます。
 *
 * プールされたデータがエラーになった場合などに使用します。
 *
 * pool: プーリング構造体のポインタ
 * data: データのポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void pool_reset(struct pool_t* p, void* data)
{
    struct pool_element_t* e;
    int n;

    CS_START(&p->critical_section);
    e = p->e;
    n = p->element_num;
    while (n--) {
        if (e->used) {
            if (e->data == data) {
                /* 解放します。*/
                (*p->cb_remove)(e->data);
                /* 作成します。*/
                e->data = (*p->cb_add)((void*)p->param);
                e->used = 0;
                e->systime = 0;
                e->last_access = system_time();
                break;
            }
        }
        e++;
    }
    CS_END(&p->critical_section);
}
