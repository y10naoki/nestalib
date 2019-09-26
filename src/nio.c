/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2008-2013 YAMAMOTO Naoki
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
#include "hdb.h"
#include "bdb.h"

/* ハッシュDBとB+木DBの抽象関数群です。
 * 関数はマルチスレッドで動作します。
 *
 * キーの長さは 1024 バイト以下に制限されています。
 * キーはバイナリで比較されます。
 * キーの比較を行う関数を外部から設定できます。
 * キー、データともに可変長で扱われます。
 * データ長の最大は31ビット(2GB)まで格納可能です。
 *
 * ハッシュDBの特徴
 * ・ハッシュ値を利用した効率的なアクセスを実現しています。
 * ・重複キーは許可されません。
 * ・キー順アクセスはサポートされません。
 *
 * B+木DBの特徴
 * ・重複キーは許可／不許可に設定できます。
 * ・重複キーのデータはリンク形式でデータ部で管理されます。
 * ・キー順アクセスをサポートしています。
 * ・リーフノードのプレフィックス圧縮に対応しています。(2013/09/23)
 *
 * エラーが発生した場合はエラーログに出力されます。
 *
 * 参考文献：bit別冊「ファイル構造」(1997)共立出版
 */

int nio_cmpkey(const void* k1, int k1size, const void* k2, int k2size)
{
    int cmpsize;
    int c;

    cmpsize = (k1size < k2size)? k1size : k2size;
    c = memcmp(k1, k2, cmpsize);
    if (c < 0)
        return -1;
    if (c == 0) {
        if (k1size == k2size)
            return 0;
        if (k1size < k2size)
            return -1;
    }
    return 1;
}

char* nio_make_filename(char* fpath, const char* basename, const char* extname)
{
    sprintf(fpath, "%s%s", basename, extname);
    return fpath;
}

int64 nio_filesize(struct nio_t* nio)
{
    return nio->mmap->real_size;
}

static int put_free_ptr(struct nio_t* nio, int64 ptr)
{
    mmap_seek(nio->mmap, NIO_FREEDATA_OFFSET);
    if (mmap_write(nio->mmap, &ptr, sizeof(int64)) != sizeof(int64)) {
        err_write("put_free_ptr: write error.");
        return -1;
    }
    nio->free_ptr = ptr;
    return 0;
}

int nio_create_free_page(struct nio_t* nio)
{
    char buf[NIO_FREEPAGE_SIZE];
    ushort rid;
    int64 last;

    memset(buf, '\0', NIO_FREEPAGE_SIZE);

    /* データ識別コード */
    rid = NIO_FREEPAGE_ID;
    memcpy(buf, &rid, sizeof(rid));

    /* ファイルの最後に書き出します。*/
    last = mmap_seek(nio->mmap, nio->mmap->real_size);

    /* 空き管理ページの書き出し */
    mmap_seek(nio->mmap, last);
    if (mmap_write(nio->mmap, buf, NIO_FREEPAGE_SIZE) != NIO_FREEPAGE_SIZE) {
        err_write("nio_create_free_page: can't write free page.");
        return -1;
    }
    return put_free_ptr(nio, last);
}

static int read_free_page(struct nio_t* nio, int64 ptr, struct nio_free_t* fpg)
{
    char buf[NIO_FREEPAGE_SIZE];
    ushort rid;
    int64 nextptr;
    ushort count;
    int i;
    char* p;

    /* 空き管理ページの読み込み */
    mmap_seek(nio->mmap, ptr);
    if (mmap_read(nio->mmap, buf, NIO_FREEPAGE_SIZE) != NIO_FREEPAGE_SIZE) {
        err_write("read_free_page: can't read free page.");
        return -1;
    }
    /* データ識別コードのチェック */
    rid = NIO_FREEPAGE_ID;
    if (memcmp(buf, &rid, sizeof(rid)) != 0) {
        err_write("read_free_page: illegal free record id.");
        return -1;
    }

    fpg->offset = ptr;

    /* 次空きデータ管理ページポインタ(8バイト) */
    memcpy(&nextptr, &buf[NIO_FREEPAGE_NEXT_OFFSET], sizeof(int64));
    fpg->next_ptr = nextptr;

    /* 領域数 */
    memcpy(&count, &buf[NIO_FREEPAGE_COUNT_OFFSET], sizeof(ushort));
    fpg->count = count;

    p = &buf[NIO_FREEPAGE_ARRAY_OFFSET];
    for (i = 0; i < fpg->count; i++) {
        int32 size;
        int64 lptr;

        memcpy(&size, p, sizeof(size));
        p += sizeof(size);
        fpg->page_size[i] = size;

        memcpy(&lptr, p, sizeof(lptr));
        p += sizeof(lptr);
        fpg->data_ptr[i] = lptr;
    }
    return 0;
}

static int write_free_page(struct nio_t* nio, struct nio_free_t* fpg)
{
    char buf[NIO_FREEPAGE_SIZE];
    ushort rid;
    int64 nextptr;
    ushort count;
    int i;
    char* p;

    memset(buf, '\0', NIO_FREEPAGE_SIZE);

    /* データ識別コード */
    rid = NIO_FREEPAGE_ID;
    memcpy(buf, &rid, sizeof(rid));

    /* 次空きデータ管理ページポインタ */
    nextptr = fpg->next_ptr;
    memcpy(&buf[NIO_FREEPAGE_NEXT_OFFSET], &nextptr, sizeof(int64));

    /* 領域数 */
    count = (ushort)fpg->count;
    memcpy(&buf[NIO_FREEPAGE_COUNT_OFFSET], &count, sizeof(ushort));

    p = &buf[NIO_FREEPAGE_ARRAY_OFFSET];
    for (i = 0; i < fpg->count; i++) {
        int32 size;
        int64 lptr;

        size = fpg->page_size[i];
        memcpy(p, &size, sizeof(size));
        p += sizeof(size);

        lptr = fpg->data_ptr[i];
        memcpy(p, &lptr, sizeof(lptr));
        p += sizeof(lptr);
    }

    /* 空き管理ページの書き出し */
    mmap_seek(nio->mmap, fpg->offset);
    if (mmap_write(nio->mmap, buf, NIO_FREEPAGE_SIZE) != NIO_FREEPAGE_SIZE) {
        err_write("write_free_page: can't write free page.");
        return -1;
    }
    return 0;
}

static int new_free_page(struct nio_t* nio,
                         struct nio_free_t* fpg,
                         int size,
                         int64 ptr)
{
    int64 fptr;

    /* ファイルの最後に割り当てます。
       reuse_space() を呼ぶと再帰呼び出しになる可能性があるため。*/
    fptr = nio->mmap->real_size;

    fpg->offset = fptr;
    fpg->count = 1;
    fpg->page_size[0] = size;
    fpg->data_ptr[0] = ptr;
    fpg->next_ptr = nio->free_ptr;

    /* ヘッダーの空きポインタを更新します。*/
    return put_free_ptr(nio, fptr);
}

int nio_add_free_list(struct nio_t* nio, int64 ptr, int size)
{
    int result = 0;
    ushort rid = NIO_FREEDATA_ID;
    struct nio_free_t* fpg;

    /* ファイルの最後を削除する場合はファイルサイズを小さくします。*/
    if (nio->mmap->real_size == ptr+size) {
        /* ファイルサイズを調整します。*/
        nio->mmap->real_size = ptr;
        return 0;
    }

    /* 解放する領域を空きデータとして更新します。*/
    mmap_seek(nio->mmap, ptr);
    if (mmap_write(nio->mmap, &rid, sizeof(rid)) != sizeof(rid)) {
        err_write("nio_add_free_list: can't mmap_write rid.");
        return -1;
    }
    if (mmap_write(nio->mmap, &size, sizeof(int)) != sizeof(int)) {
        err_write("nio_add_free_list: can't mmap_write size.");
        return -1;
    }

    /* 空きデータ管理部の更新 */
    fpg = nio->free_page;
    if (nio->free_ptr == 0) {
        /* 新しいページに追加します。*/
        result = new_free_page(nio, fpg, size, ptr);
    } else {
        /* 空き領域管理ページの読み込み */
        /* すべてのページを探すと遅いので最初のページのみチェックします。*/
        result = read_free_page(nio, nio->free_ptr, fpg);
        if (result == 0) {
            if (fpg->count < NIO_FREE_COUNT) {
                fpg->page_size[fpg->count] = size;
                fpg->data_ptr[fpg->count] = ptr;
                fpg->count++;
            } else {
                /* 新しいページに追加します。*/
                result = new_free_page(nio, fpg, size, ptr);
            }
        }
    }
    if (result == 0) {
        /* 空き領域管理ページの書き出し */
        result = write_free_page(nio, fpg);
    }
    return result;
}

static int is_divide_space(int freesize, int size, int filling_rate)
{
    int remain;
    int rate;

    remain = freesize - size;
    if (remain <= 64)
        return 0;  /* 残りが64バイト以下は分割利用しません */
    rate = (freesize - size) * 100 / size;
    return (rate > filling_rate);
}

static int unlink_free_list(struct nio_t* nio, int64 del_ptr, int64 next_ptr)
{
    int64 fptr;

    fptr = nio->free_ptr;
    while (fptr != 0) {
        int64 nxptr;
        int64 offset;

        offset = fptr + NIO_FREEPAGE_NEXT_OFFSET;
        mmap_seek(nio->mmap, offset);
        if (mmap_read(nio->mmap, &nxptr, sizeof(int64)) != sizeof(int64)) {
            err_write("unlink_free_list: can't mmap_read.");
            return -1;
        }
        if (nxptr == del_ptr) {
            mmap_seek(nio->mmap, offset);
            if (mmap_write(nio->mmap, &next_ptr, sizeof(int64)) != sizeof(int64)) {
                err_write("unlink_free_list: can't mmap_write.");
                return -1;
            }
            break;
        }
        fptr = nxptr;
    }
    return 0;
}

static int64 reuse_space(struct nio_t* nio, int size, int* areasize, int filling_rate)
{
    int64 fptr;
    struct nio_free_t* fpg;
    int result = 0;

    fpg = nio->free_page;

    /* 空き領域管理ページの読み込み */
    fptr = nio->free_ptr;
    while (fptr != 0 && result == 0) {
        result = read_free_page(nio, fptr, fpg);
        if (result == 0) {
            int i;

            for (i = 0; i < fpg->count; i++) {
                if (fpg->page_size[i] >= size) {
                    int64 ptr;

                    /* 格納可能な空き領域が見つかった */
                    /* 空き領域を分割するか判定します。*/
                    if (is_divide_space(fpg->page_size[i], size, filling_rate)) {
                        int rest_size;

                        /* 空き領域を分割する場合は後半を再利用します。*/
                        rest_size = fpg->page_size[i] - size;
                        ptr = fpg->data_ptr[i] + rest_size;
                        if (areasize != NULL)
                            *areasize = size;
                        /* 空き管理データを更新します。*/
                        fpg->page_size[i] = rest_size;
                        /* 空きデータの領域サイズを更新します。*/
                        mmap_seek(nio->mmap, fpg->data_ptr[i]+sizeof(int));
                        if (mmap_write(nio->mmap, &rest_size, sizeof(int)) != sizeof(int)) {
                            err_write("reuse_space: can't mmap write.");
                            return -1;
                        }
                    } else {
                        ptr = fpg->data_ptr[i];
                        if (areasize != NULL)
                            *areasize = fpg->page_size[i];
                        fpg->count--;

                        /* fpg->countがゼロになった場合はファイルの最後の時のみ
                           ファイルサイズを調整します。
                           それ以外は空き管理ページを解放する。*/
                        if (fpg->count == 0) {
                            if (nio->free_ptr == fptr) {
                                /* 空き領域管理ポインタを更新します。*/
                                if (put_free_ptr(nio, fpg->next_ptr) < 0)
                                    return -1;
                            }
                            if (nio->mmap->real_size == fptr+NIO_FREEPAGE_SIZE) {
                                /* ファイルサイズを調整します。*/
                                nio->mmap->real_size = fptr;
                            } else {
                                /* フリーリストのリンクをつなぎ変えます。*/
                                if (unlink_free_list(nio, fpg->offset, fpg->next_ptr) < 0)
                                    return -1;
                                /* 領域を開放します。*/
                                if (nio_add_free_list(nio, fpg->offset, NIO_FREEPAGE_SIZE) < 0)
                                    return -1;
                            }
                            return ptr;
                        }
                        if (i < fpg->count) {
                            /* 空き領域を左シフトします。*/
                            int shift_n;

                            shift_n = fpg->count - i;
                            memmove(&fpg->page_size[i], &fpg->page_size[i+1], shift_n * sizeof(int));
                            memmove(&fpg->data_ptr[i], &fpg->data_ptr[i+1], shift_n * sizeof(int64));
                            fpg->page_size[fpg->count] = 0;
                            fpg->data_ptr[fpg->count] = 0;
                        }
                    }
                    if (write_free_page(nio, fpg) < 0)
                        return -1;

                    return ptr; /* found space */
                }
            }
            fptr = fpg->next_ptr;
        }
    }
    return -1;  /* not found */
}

int64 nio_avail_space(struct nio_t* nio, int size, int* areasize, int filling_rate)
{
    int64 offset = -1;

    if (nio->free_ptr != 0) {
        /* 空き領域管理ページから再利用できる領域を検索します。*/
        offset = reuse_space(nio, size, areasize, filling_rate);
        if (offset > 0)
            mmap_seek(nio->mmap, offset);
    }

    if (offset < 0) {
        /* 空き領域はないため、ファイルの最後に追加します。*/
        offset = mmap_seek(nio->mmap, nio->mmap->real_size);
        if (areasize != NULL)
            *areasize = size;
    }
    return offset;
}

/*
 * データベースオブジェクトを作成します。
 *
 * dbtype: データベースタイプ
 *         NIO_HASH:  ハッシュデータベース
 *         NIO_BTREE: B+木データベース
 *
 * 戻り値
 *  データベースオブジェクトのポインタを返します。
 *  メモリ不足の場合は NULL を返します。
 */
struct nio_t* nio_initialize(int dbtype)
{
    struct nio_t* nio;

    nio = (struct nio_t*)calloc(1, sizeof(struct nio_t));
    if (nio == NULL) {
        err_write("nio_initialize: no memory.");
        return NULL;
    }
    nio->dbtype = dbtype;

    nio->free_page = (struct nio_free_t*)calloc(1, sizeof(struct nio_free_t));
    if (nio->free_page == NULL) {
        err_write("nio_initialize: no memory.");
        free(nio);
        return NULL;
    }

    /* 関数の設定 */
    if (dbtype == NIO_HASH) {
        nio->db = hdb_initialize(nio);

        nio->finalize_func = (FINALIZE_FUNCPTR)hdb_finalize;
        nio->property_func = (PROPERTY_FUNCPTR)hdb_property;
        nio->open_func = (OPEN_FUNCPTR)hdb_open;
        nio->create_func = (CREATE_FUNCPTR)hdb_create;
        nio->close_func = (CLOSE_FUNCPTR)hdb_close;
        nio->file_func = (FILE_FUNCPTR)hdb_file;
        nio->find_func = (FIND_FUNCPTR)hdb_find;
        nio->get_func = (GET_FUNCPTR)hdb_get;
        nio->gets_func = (GETS_FUNCPTR)hdb_gets;
        nio->aget_func = (AGET_FUNCPTR)hdb_aget;
        nio->agets_func = (AGETS_FUNCPTR)hdb_agets;
        nio->put_func = (PUT_FUNCPTR)hdb_put;
        nio->puts_func = (PUTS_FUNCPTR)hdb_puts;
        nio->bset_func = (BSET_FUNCPTR)hdb_bset;
        nio->delete_func = (DELETE_FUNCPTR)hdb_delete;
        nio->free_func = (FREE_FUNCPTR)hdb_free;

        nio->cursor_open_func = (CURSOR_OPEN_FUNCPTR)hdb_cursor_open;
        nio->cursor_close_func = (CURSOR_CLOSE_FUNCPTR)hdb_cursor_close;
        nio->cursor_next_func = (CURSOR_NEXT_FUNCPTR)hdb_cursor_next;
        nio->cursor_key_func = (CURSOR_KEY_FUNCPTR)hdb_cursor_key;
    } else if (dbtype == NIO_BTREE) {
        nio->db = bdb_initialize(nio);

        nio->finalize_func = (FINALIZE_FUNCPTR)bdb_finalize;
        nio->property_func = (PROPERTY_FUNCPTR)bdb_property;
        nio->open_func = (OPEN_FUNCPTR)bdb_open;
        nio->create_func = (CREATE_FUNCPTR)bdb_create;
        nio->close_func = (CLOSE_FUNCPTR)bdb_close;
        nio->file_func = (FILE_FUNCPTR)bdb_file;
        nio->find_func = (FIND_FUNCPTR)bdb_find;
        nio->get_func = (GET_FUNCPTR)bdb_get;
        nio->aget_func = (AGET_FUNCPTR)bdb_aget;
        nio->put_func = (PUT_FUNCPTR)bdb_put;
        nio->delete_func = (DELETE_FUNCPTR)bdb_delete;
        nio->free_func = (FREE_FUNCPTR)bdb_free;

        nio->cursor_open_func = (CURSOR_OPEN_FUNCPTR)bdb_cursor_open;
        nio->cursor_close_func = (CURSOR_CLOSE_FUNCPTR)bdb_cursor_close;
        nio->cursor_next_func = (CURSOR_NEXT_FUNCPTR)bdb_cursor_next;
        nio->cursor_nextkey_func = (CURSOR_NEXT_FUNCPTR)bdb_cursor_nextkey;
        nio->cursor_prev_func = (CURSOR_PREV_FUNCPTR)bdb_cursor_prev;
        nio->cursor_prevkey_func = (CURSOR_PREV_FUNCPTR)bdb_cursor_prevkey;
        nio->cursor_duplicate_last_func = (CURSOR_DUPLICATE_LAST_FUNCPTR)bdb_cursor_duplicate_last;
        nio->cursor_find_func = (CURSOR_FIND_FUNCPTR)bdb_cursor_find;
        nio->cursor_seek_func = (CURSOR_SEEK_FUNCPTR)bdb_cursor_seek;
        nio->cursor_key_func = (CURSOR_KEY_FUNCPTR)bdb_cursor_key;
        nio->cursor_value_func = (CURSOR_VALUE_FUNCPTR)bdb_cursor_value;
        nio->cursor_update_func = (CURSOR_UPDATE_FUNCPTR)bdb_cursor_update;
        nio->cursor_delete_func = (CURSOR_DELETE_FUNCPTR)bdb_cursor_delete;
    } else {
        err_write("nio_initialize: dbtype error=%d.", dbtype);
        free(nio->free_page);
        free(nio);
        return NULL;
    }

    if (nio->db == NULL) {
        err_write("nio_initialize: error.");
        free(nio->free_page);
        free(nio);
        return NULL;
    }
    return nio;
}

/*
 * データベースオブジェクトを解放します。
 * 確保されていた領域が解放されます。
 *
 * nio: データベースオブジェクトのポインタ
 *
 * 戻り値
 *  なし
 */
void nio_finalize(struct nio_t* nio)
{
    if (nio == NULL)
        return;
    (*nio->finalize_func)(nio->db);
    if (nio->free_page)
        free(nio->free_page);
    free(nio);
}

/*
 * キーの比較を行う関数を設定します。
 *
 * キー比較関数のプロトタイプ
 * int CMPFUNC(const void* key, int key1size, const void* key2, int key2size);
 *
 * key < key2 の場合は負の値を返します。
 * key == key2 の場合は 0 を返します。
 * key > key2 の場合は正の値を返します。
 *
 * nio: データベースオブジェクトのポインタ
 * func: 関数のポインタ
 *
 * 戻り値
 *  なし
 */
void nio_cmpfunc(struct nio_t* nio, CMP_FUNCPTR func)
{
    if (nio->dbtype == NIO_HASH) {
        struct hdb_t* hdb;

        hdb = (struct hdb_t*)nio->db;
        hdb_cmpfunc(hdb, func);
    } else if (nio->dbtype == NIO_BTREE) {
        struct bdb_t* bdb;

        bdb = (struct bdb_t*)nio->db;
        bdb_cmpfunc(bdb, func);
    }
}

/*
 * ハッシュデータベースで使用するハッシュ関数を設定します。
 *
 * ハッシュ関数のプロトタイプは以下の通りです。
 * unsigned int HASHFUNC(const void* key, int len, unsigned int seed);
 *
 * デフォルトでは MurmurHash2A が使用されます。
 *
 * nio: データベースオブジェクトのポインタ
 * func: ハッシュ関数のポインタ
 *
 * 戻り値
 *  なし
 */
void nio_hashfunc(struct nio_t* nio, HASH_FUNCPTR func)
{
    struct hdb_t* hdb;

    if (nio->dbtype != NIO_HASH)
        return;
    hdb = (struct hdb_t*)nio->db;
    hdb_hashfunc(hdb, func);
}

/*
 * データベースのプロパティを設定します。
 *
 * プロパティ種類：
 *   [Hash]
 *     NIO_BUCKET_NUM        バケット配列数
 *     NIO_MAP_VIEWSIZE      マップサイズ
 *     NIO_ALIGN_BYTES       キーデータ境界サイズ
 *     NIO_FILLING_RATE      データ充填率
 *   [B+Tree]
 *     NIO_PAGESIZE          ノードページサイズ
 *     NIO_MAP_VIEWSIZE      マップサイズ
 *     NIO_ALIGN_BYTES       キーデータ境界サイズ
 *     NIO_FILLING_RATE      データ充填率
 *     NIO_DUPLICATE_KEY     キー重複を許可(1 or 0)
 *     NIO_DATAPACK          データパック(1 or 0)
 *     NIO_PREFIX_COMPRESS   プレフィックス圧縮(1 or 0)
 *
 * nio: データベースオブジェクトのポインタ
 * kind: プロパティ種類
 * value: 値
 *
 * 戻り値
 *  設定した場合はゼロを返します。エラーの場合は -1 を返します。
 */
int nio_property(struct nio_t* nio, int kind, int value)
{
    if (nio == NULL)
        return -1;
    return (*nio->property_func)(nio->db, kind, value);
}

/*
 * データベースファイルをオープンします。
 *
 * nio: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  オープンできた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int nio_open(struct nio_t* nio, const char* fname)
{
    if (nio == NULL)
        return -1;
    return (*nio->open_func)(nio->db, fname);
}

/*
 * データベースファイルを新規に作成します。
 * ファイルがすでに存在する場合でも新規に作成されます。
 *
 * nio: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  オープンできた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int nio_create(struct nio_t* nio, const char* fname)
{
    int result;

    if (nio == NULL)
        return -1;
    result = (*nio->create_func)(nio->db, fname);
    if (result == 0)
        nio->free_ptr = 0;    // 2012.8.21
    return result;
}

/*
 * データベースファイルをクローズします。
 * 設定されているプロパティは初期化されます。
 *
 * nio: データベースオブジェクトのポインタ
 *
 * 戻り値
 *  なし
 */
void nio_close(struct nio_t* nio)
{
    if (nio)
        (*nio->close_func)(nio->db);
}

/*
 * データベースが存在するか調べます。
 *
 * nio: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  データベースがが存在する場合は 1 を返します。
 *  存在しない場合はゼロを返します。
 */
int nio_file(struct nio_t* nio, const char* fname)
{
    if (nio == NULL)
        return -1;
    return (*nio->file_func)(fname);
}

/*
 * データベースからキーを検索します。
 * 重複キーが許可されているデータベースの場合は
 * 最初のキーのサイズを返します。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 *
 * キー値を存在していた場合は値のサイズを返します。
 * キーが存在しない場合は -1 を返します。
 */
int nio_find(struct nio_t* nio, const void* key, int keysize)
{
    if (nio == NULL)
        return -1;
    return (*nio->find_func)(nio->db, key, keysize);
}

/*
 * データベースからキーを検索して値をポインタに設定します。
 * 重複キーが許可されているデータベースの場合は
 * 最初のキーの値を取得します。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値の領域サイズ
 *
 * キー値を取得できた場合は値のサイズを返します。
 * キーが存在しない場合は -1 を返します。
 * 値の領域が不足している場合は -2 を返します。
 * その他のエラーの場合は負の値を返します。
 */
int nio_get(struct nio_t* nio, const void* key, int keysize, void* val, int valsize)
{
    if (nio == NULL)
        return -1;
    return (*nio->get_func)(nio->db, key, keysize, val, valsize);
}

/*
 * データベースからキーを検索して値をポインタに設定します。
 * 楽観的排他制御を実現するためのユニークな値を取得します。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値の領域サイズ
 * cas: 楽観的排他制御のための値のポインタ
 *
 * キー値を取得できた場合は値のサイズを返します。
 * キーが存在しない場合は -1 を返します。
 * 値の領域が不足している場合は -2 を返します。
 * その他のエラーの場合は負の値を返します。
 */
int nio_gets(struct nio_t* nio, const void* key, int keysize, void* val, int valsize, int64* cas)
{
    if (nio == NULL)
        return -1;
    if (nio->dbtype != NIO_HASH)
        return -1;
    return (*nio->gets_func)(nio->db, key, keysize, val, valsize, cas);
}

/*
 * データベースからキーを検索して値のポインタを返します。
 * 値の領域は関数内で確保されます。
 * 値のポインタは使用後に nio_free() で解放する必要があります。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * valsize: 値の領域サイズが設定されるポインタ
 *
 * キー値を取得できた場合は値のサイズを設定して領域のポインタを返します。
 * キーが存在しない場合は valsize に -1 が設定されて NULL を返します。
 * その他のエラーの場合は valsize に -2 が設定されて NULL を返します。
 */
void* nio_aget(struct nio_t* nio, const void* key, int keysize, int* valsize)
{
    if (nio == NULL)
        return NULL;
    return (*nio->aget_func)(nio->db, key, keysize, valsize);
}

/*
 * データベースからキーを検索して値のポインタを返します。
 * 値の領域は関数内で確保されます。
 * 値のポインタは使用後に nio_free() で解放する必要があります。
 * 楽観的排他制御を実現するためのユニークな値を取得します。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * valsize: 値の領域サイズが設定されるポインタ
 * cas: 楽観的排他制御のための値のポインタ
 *
 * キー値を取得できた場合は値のサイズを設定して領域のポインタを返します。
 * キーが存在しない場合は valsize に -1 が設定されて NULL を返します。
 * その他のエラーの場合は valsize に -2 が設定されて NULL を返します。
 */
void* nio_agets(struct nio_t* nio, const void* key, int keysize, int* valsize, int64* cas)
{
    if (nio == NULL)
        return NULL;
    if (nio->dbtype != NIO_HASH)
        return NULL;
    return (*nio->agets_func)(nio->db, key, keysize, valsize, cas);
}

/*
 * データベースにキーと値を設定します。
 *
 * 重複キーが許可されていないデータベースの場合で
 * キーがすでに存在している場合は値が置換されます。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値のサイズ
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int nio_put(struct nio_t* nio, const void* key, int keysize, const void* val, int valsize)
{
    if (nio == NULL)
        return -1;
    return (*nio->put_func)(nio->db, key, keysize, val, valsize);
}

/*
 * データベースにキーと値を設定します。
 *
 * 重複キーが許可されていないデータベースの場合で
 * キーがすでに存在している場合は値が置換されます。
 * データを更新する場合は楽観的排他制御のチェックが行われます。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値のサイズ
 * cas: 楽観的排他制御のための値
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 * 他のユーザーがすでに更新していた場合は -2 を返します。
 */
int nio_puts(struct nio_t* nio, const void* key, int keysize, const void* val, int valsize, int64 cas)
{
    if (nio == NULL)
        return -1;
    if (nio->dbtype != NIO_HASH)
        return -1;
    return (*nio->puts_func)(nio->db, key, keysize, val, valsize, cas);
}

/*
 * データベースにキーと値を設定します。
 * この関数はレプリケーションで使用されます。
 *
 * 重複キーが許可されていないデータベースの場合で
 * キーがすでに存在している場合は値が置換されます。
 * 楽観的排他制御のための値も更新されます。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値のサイズ
 * cas: 楽観的排他制御のための値
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int nio_bset(struct nio_t* nio, const void* key, int keysize, const void* val, int valsize, int64 cas)
{
    if (nio == NULL)
        return -1;
    if (nio->dbtype != NIO_HASH)
        return -1;
    return (*nio->bset_func)(nio->db, key, keysize, val, valsize, cas);
}

/*
 * データベースからキーを削除します。
 * 重複キーが許可されれているデータベースの場合は
 * すべて削除されます。
 * 削除された領域は再利用されます。
 *
 * nio: データベースオブジェクトのポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int nio_delete(struct nio_t* nio, const void* key, int keysize)
{
    if (nio == NULL)
        return -1;
    return (*nio->delete_func)(nio->db, key, keysize);
}


/*
 * 関数内で確保したメモリ領域を開放します。
 *
 * nio: データベースオブジェクトのポインタ
 * v: 領域のポインタ
 */
void nio_free(struct nio_t* nio, const void* v)
{
    if (nio)
        (*nio->free_func)(v);
}

/*
 * オープンされているデータベースファイルからキー順アクセスするための
 * カーソルを作成します。
 * キー位置は先頭に位置づけられます。
 *
 * nio: データベースオブジェクトのポインタ
 *
 * 成功した場合はカーソル構造体のポインタを返します。
 * エラーの場合は NULL を返します。
 */
struct nio_cursor_t* nio_cursor_open(struct nio_t* nio)
{
    struct nio_cursor_t* cur;

    if (nio == NULL)
        return NULL;

    cur = malloc(sizeof(struct nio_cursor_t));
    if (cur == NULL) {
        err_write("nio_cursor_open: no memory.");
        return NULL;
    }
    cur->dbtype = nio->dbtype;
    cur->nio = nio;
    cur->cursor = (*nio->cursor_open_func)(nio->db);
    return cur;
}

/*
 * カーソルをクローズします。
 * カーソル領域は解放されます。
 *
 * cur: カーソル構造体のポインタ
 *
 * 戻り値 なし
 */
void nio_cursor_close(struct nio_cursor_t * cur)
{
    if (cur == NULL)
        return;

    (*cur->nio->cursor_close_func)(cur->cursor);
    free(cur);
}

/*
 * カーソルの現在位置を次に進めます。
 * 重複キーの場合は次の値に現在位置が移動します。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に移動できた場合はゼロが返されます。
 * カーソルが終わりの場合は NIO_CURSOR_END が返されます。
 * エラーの場合は -1 が返されます。
*/
int nio_cursor_next(struct nio_cursor_t* cur)
{
    if (cur == NULL)
        return -1;

    return (*cur->nio->cursor_next_func)(cur->cursor);
}

/*
 * カーソルの現在位置を次に進めます。
 * 重複キーの場合でも次のキーに現在位置が移動します。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に移動できた場合はゼロが返されます。
 * カーソルが終わりの場合は NIO_CURSOR_END が返されます。
 * エラーの場合は -1 が返されます。
 */
int nio_cursor_nextkey(struct nio_cursor_t* cur)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;
    
    return (*cur->nio->cursor_nextkey_func)(cur->cursor);
}

/*
 * カーソルの現在位置を前に進めます。
 * 重複キーの場合は前の値に現在位置が移動します。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に移動できた場合はゼロが返されます。
 * カーソルの現在位置が先頭の場合は NIO_CURSOR_END が返されます。
 * エラーの場合は -1 が返されます。
 */
int nio_cursor_prev(struct nio_cursor_t* cur)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;

    return (*cur->nio->cursor_prev_func)(cur->cursor);
}

/*
 * カーソルの現在位置を前に進めます。
 * 重複キーの場合でも前のキーに現在位置が移動します。
 *
 * cur: カーソル構造体のポインタ
 * 
 * 正常に移動できた場合はゼロが返されます。
 * カーソルの現在位置が先頭の場合は NIO_CURSOR_END が返されます。
 * エラーの場合は -1 が返されます。
 */
int nio_cursor_prevkey(struct nio_cursor_t* cur)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;
    
    return (*cur->nio->cursor_prevkey_func)(cur->cursor);
}

/*
 * カーソルの現在位置を重複キーの最後に移動させます。
 * 重複索引でない場合は現在位置は変わりません。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に移動できた場合はゼロが返されます。
 * エラーの場合は -1 が返されます。
 */
int nio_cursor_duplicate_last(struct nio_cursor_t* cur)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;
    
    return (*cur->nio->cursor_duplicate_last_func)(cur->cursor);
}

/*
 * カーソルの現在位置をキーと条件の位置に移動します。
 * cond には以下の定義を指定できます。
 *
 * BDB_COND_EQ (=)
 * BDB_COND_GT (>)
 * BDB_COND_GE (>=)
 * BDB_COND_LT (<)
 * BDB_COND_LE (<=)
 *
 * cur: カーソル構造体のポインタ
 * cond: 条件
 * key: キー
 * keysize: キーサイズ
 *
 * 正常に処理された場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int nio_cursor_find(struct nio_cursor_t* cur, int cond, const void* key, int keysize)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;

    return (*cur->nio->cursor_find_func)(cur->cursor, cond, key, keysize);
}

/*
 * カーソルの現在位置を pos に移動します。
 * pos には BDB_SEEK_TOP（先頭）か BDB_SEEK_BOTTOM（末尾）を指定します。
 *
 * cur: カーソル構造体のポインタ
 * pos: 位置
 *
 * 正常に処理された場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int nio_cursor_seek(struct nio_cursor_t* cur, int pos)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;

    return (*cur->nio->cursor_seek_func)(cur->cursor, pos);
}

/*
 * カーソルの現在位置からキーを取得します。
 * keysizeにはキーが設定される領域の大きさを指定します。
 *
 * cur: カーソル構造体のポインタ
 * key: キー領域のポインタ
 * keysize: キー領域のサイズ
 *
 * 正常に取得された場合はキーのサイズを返します。
 * エラーの場合は -1 を返します。
 */
int nio_cursor_key(struct nio_cursor_t* cur, void* key, int keysize)
{
    if (cur == NULL)
        return -1;

    return (*cur->nio->cursor_key_func)(cur->cursor, key, keysize);
}

/* カーソルの現在位置から値を取得します。
 * valsizeには値が設定される領域の大きさを指定します。
 *
 * cur: カーソル構造体のポインタ
 * val: 値領域のポインタ
 * valsize: 値領域のサイズ
 *
 * 正常に取得された場合は値のサイズを返します。
 * エラーの場合は -1 を返します。
 */
int nio_cursor_value(struct nio_cursor_t* cur, void* val, int valsize)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;

    return (*cur->nio->cursor_value_func)(cur->cursor, val, valsize);
}

/*
 * カーソルの現在位置の値を val で更新します。
 *
 * cur: カーソル構造体のポインタ
 * val: 値領域のポインタ
 * valsize: 値領域のサイズ
 *
 * 正常に更新された場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int nio_cursor_update(struct nio_cursor_t* cur, const void* val, int valsize)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;

    return (*cur->nio->cursor_update_func)(cur->cursor, val, valsize);
}

/*
 * カーソルの現在位置のキーと値を削除します。
 * 重複キーの場合で削除後もまだキーが存在する場合は、
 * 現在位置の値だけが削除されます。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に削除された場合はゼロを返します。
 * 削除は正常に行えたが現在位置が不定の場合は 1 を返します。
 * エラーの場合は-1を返します。
 */
int nio_cursor_delete(struct nio_cursor_t* cur)
{
    if (cur == NULL)
        return -1;
    if (cur->dbtype != NIO_BTREE)
        return -1;

    return (*cur->nio->cursor_delete_func)(cur->cursor);
}
