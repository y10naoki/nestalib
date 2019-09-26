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
#include "fcache.h"

#define INC_ELEMENT_NUM  10  /* 要素数増分値 */

/*
 * ファイルキャッシュを使用可能にします。
 *
 * 指定されたキャッシュサイズ容量まで自動的に拡張するキャッシュの
 * 初期化を行ないます。
 *
 * cache_size: キャッシュサイズ容量（バイト数）
 *
 * 戻り値
 *  なし
 */
APIEXPORT struct file_cache_t* fc_initialize(int cache_size)
{
    struct file_cache_t* fc;

    /* 識別構造体の確保 */
    fc = (struct file_cache_t*)calloc(1, sizeof(struct file_cache_t));
    if (fc == NULL) {
        err_write("fcache: No memory.");
        return NULL;
    }

    /* クリティカルセクションの初期化 */
    CS_INIT(&fc->cache_critical_section);

    /* 最大キャッシュサイズ（バイト数）の設定 */
    fc->max_cache_size = cache_size;

    /* 現在のキャッシュサイズの初期化 */
    fc->count = 0;
    fc->cur_cache_size = 0;

    fc->capacity = INC_ELEMENT_NUM;
    fc->element_list = (struct cache_element_t*)calloc(fc->capacity, sizeof(struct cache_element_t));
    return fc;
}

/*
 * ファイルキャッシュの使用を終了します。
 *
 * 動的に確保された領域が解放されます。
 *
 * fc: ファイルキャッシュ構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void fc_finalize(struct file_cache_t* fc)
{
    struct cache_element_t* e;
    int i;

    if (fc == NULL)
        return;

    /* 領域の解放 */
    CS_START(&fc->cache_critical_section);
    e = fc->element_list;
    for (i = 0; i < fc->count; i++) {
        if (e->data != NULL)
            free(e->data);
        e++;
    }
    free(fc->element_list);
    CS_END(&fc->cache_critical_section);

    /* クリティカルセクションの削除 */
    CS_DELETE(&fc->cache_critical_section);

    free(fc);
}

/*
 * キャッシュからデータを取得します。
 * キャッシュが最新ではない場合は削除フラグが設定されてキャッシュ設定で削除されます。
 *
 * fc: ファイルキャッシュ構造体のポインタ
 * fpath: ファイルのフルパス
 * ts: ファイルの最終更新日
 * fsize: ファイルサイズ
 *
 * 戻り値
 *  キャッシュデータのポインタを返します。
 *  キャッシュに存在しない場合やキャッシュが最新ではない場合は NULL を返します。
 */
APIEXPORT char* fc_get(struct file_cache_t* fc, const char* fpath, time_t ts, int fsize)
{
    struct cache_element_t* e;
    char* data = NULL;
    int i;

    CS_START(&fc->cache_critical_section);
    e = fc->element_list;
    for (i = 0; i < fc->count; i++) {
        if (strcmp(e->fpath, fpath) == 0) {
            if (e->fsize != fsize || e->ts != ts) {
                /* 最新ではない */
                e->del_flag = 1;
            } else {
                data = e->data;
                e->ref_c++;    /* 参照カウントをインクリメント */
            }
            break;
        }
        e++;
    }
    CS_END(&fc->cache_critical_section);
    return data;
}

/*
 * 削除対象フラグがマークされているキャッシュを削除します。
 *
 * この関数はクリティカルセクション中に実行されます。
 */
static void fc_delete(struct file_cache_t* fc)
{
    struct cache_element_t* e;
    int i;

    e = fc->element_list;
    for (i = 0; i < fc->count; i++) {
        if (e->del_flag) {
            int shift_n;

            /* キャッシュ領域の解放 */
            if (e->data != NULL)
                free(e->data);
            /* キャッシュ容量の減算 */
            fc->cur_cache_size -= e->fsize;

            /* 削除対象のシフト */
            shift_n = fc->count - (i + 1);
            if (shift_n > 0)
                memmove(e, e+1, shift_n * sizeof(struct cache_element_t));

            fc->count--;
            memset(fc->element_list+fc->count, '\0', sizeof(struct cache_element_t));
        } else {
            e++;
        }
    }
}

/*
 * キャッシュ・アウトする対象に削除対象フラグをマークします。
 *
 * この関数はクリティカルセクション中に実行されます。
 */
static int fc_cache_out(struct file_cache_t* fc, int fsize)
{
    struct cache_element_t* e;
    int i;
    int ref_c = -1;
    int n = 0;

    /* キャッシュ・アウトするデータがあるか調べます。*/
    e = fc->element_list;
    for (i = 0; i < fc->count; i++) {
        if (e->fsize >= fsize) {
            if (ref_c < 0) {
                ref_c = e->ref_c;
            } else {
                if (e->ref_c < ref_c)
                    ref_c = e->ref_c;
            }
        }
        e++;
    }
    if (ref_c >= 0) {
        e = fc->element_list;
        for (i = 0; i < fc->count; i++) {
            if (e->fsize >= fsize) {
                if (e->ref_c <= ref_c) {
                    e->del_flag = 1;
                    n++;
                    break;
                }
            }
            e++;
        }
    }

    if (n == 0) {
        int del_size = 0;
        /* キャッシュ・アウト対象がないため、サイズに関係なく
           参照カウントが小さいものを削除対象にします。*/
        e = fc->element_list;
        for (i = 0; i < fc->count; i++) {
            if (e->ref_c <= ref_c) {
                if (del_size >= fsize)
                    break;
                e->del_flag = 1;
                del_size += e->fsize;
                n++;
            }
            e++;
        }
    }
    return n;
}

/*
 * キャッシュにデータをセットします。
 * キャッシュデータが制限値を超える場合はファイルサイズが
 * これより大きくて参照カウントが少ないものと置き換えます。
 *
 * fc: ファイルキャッシュ構造体のポインタ
 * fpath: ファイルのフルパス
 * ts: ファイルの最終更新日
 * fsize: ファイルサイズ
 * data: データのポインタ
 *
 * 戻り値
 *  キャッシュに設定できた場合は 1 を返します。
 *  それ以外の場合はゼロを返します。
 */
APIEXPORT int fc_set(struct file_cache_t* fc, const char* fpath, time_t ts, int fsize, const char* data)
{
    struct cache_element_t* e;
    int i;
    int file_found = 0;
    int result = 0;

    CS_START(&fc->cache_critical_section);
    e = fc->element_list;
    for (i = 0; i < fc->count; i++) {
        if (strcmp(e->fpath, fpath) == 0) {
            /* キャッシュ内に存在している場合 */
            file_found = 1;
            if (fc->cur_cache_size - e->fsize + fsize <= fc->max_cache_size) {
                if (fsize > e->fsize) {
                    char* t = (char*)realloc(e->data, fsize);
                    if (t == NULL) {
                        err_write("fcache: No memory.");
                        break;
                    }
                    e->data = t;
                }
                memcpy(e->data, data, fsize);
                e->fsize = fsize;
                e->ts = ts;
                e->del_flag = 0;
                /* 参照カウントはクリアしません。*/
                fc->cur_cache_size += fsize - e->fsize;
                result = 1;
            }
            break;
        }
        e++;
    }

    if (! file_found) {
        /* キャッシュには存在してない場合 */
        /* 最新でないキャッシュを削除します。*/
        fc_delete(fc);

        if (fc->cur_cache_size + fsize > fc->max_cache_size) {
            /* サイズ制限を越えているため、参照が少なく
               ファイルサイズの大きいものをキャッシュから削除します。*/
            if (fc_cache_out(fc, fsize) > 0)
                fc_delete(fc);
        }
        if (fc->cur_cache_size + fsize <= fc->max_cache_size) {
            e = NULL;
            if (fc->count+1 > fc->capacity) {
                struct cache_element_t* t;

                /* 要素を増分します。*/
                fc->capacity += INC_ELEMENT_NUM;
                t = (struct cache_element_t*)realloc(fc->element_list, fc->capacity * sizeof(struct cache_element_t));
                if (t == NULL) {
                    err_write("fcache: No memory.");
                    fc->capacity -= INC_ELEMENT_NUM;
                } else {
                    fc->element_list = t;
                    e = fc->element_list + fc->count;
                }
            } else {
                e = fc->element_list + fc->count;
            }
            if (e != NULL) {
                strcpy(e->fpath, fpath);
                e->data = (char*)malloc(fsize);
                if (e->data != NULL)
                    memcpy(e->data, data, fsize);
                e->fsize = fsize;
                e->ts = ts;
                e->ref_c = 0;
                e->del_flag = 0;
                fc->cur_cache_size += fsize;
                fc->count++;
                result = 1;
            }
        }
    }
    CS_END(&fc->cache_critical_section);
    return result;
}
