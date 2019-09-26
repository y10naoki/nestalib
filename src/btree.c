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

/* B-Treeファイルを扱う関数群です。
 * 関数はマルチスレッドで動作します。
 *
 * キー値はバイナリで比較されますので登録や検索を行なう場合は
 * 作成時に指定したキー長を超えないようにします。
 * キー長が短い場合はゼロ(0x00)がパディングされます。
 *
 * B-Treeファイルはキーファイルとデータファイルの２ファイルから構成されます。
 * キーファイルの拡張子は .nky になります。
 * データファイルの拡張子は .ndt になります。
 * sample という B-Treeファイルが作成されると sample.nky sample.ndt の
 * ２つのファイルが作成されます。
 *
 * エラーはエラーログに出力されます。
 *
 * 参考文献：bit別冊「ファイル構造」(1997)共立出版
 */

/*
 * B-Treeファイルをオープンします。
 * B-Treeで管理されるデータファイルもオープンされます。
 * ファイル名には拡張子なしのベース名を指定します。
 * ファイル名には必要に応じてパスを指定できます。
 *
 * filename: ファイル名のポインタ
 *
 * 戻り値
 *  B-Treeの構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct btree_t* btopen(const char* filename, int key_cache_size) {
    struct btree_t* bt;
    char fpath[MAX_PATH+1];
    struct btkey_t* btkey;
    struct dio_data_t* btdat;

    if (strlen(filename)+4 > MAX_PATH) {
        err_write("btopen: filename is too long.");
        return NULL;
    }
    bt = (struct btree_t*)calloc(1, sizeof(struct btree_t));
    if (bt == NULL) {
        err_write("btopen: no memory.");
        return NULL;
    }

    /* キーファイル */
    sprintf(fpath, "%s%s", filename, KEY_FILE_EXT);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    btkey = btk_open(fpath, key_cache_size);
    if (btkey == NULL) {
        free(bt);
        return NULL;
    }

    /* データファイル */
    sprintf(fpath, "%s%s", filename, DATA_FILE_EXT);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    btdat = dio_open(fpath);
    if (btdat == NULL) {
        btk_close(btkey);
        free(bt);
        return NULL;
    }

    /* クリティカルセクションの初期化 */
    CS_INIT(&bt->critical_section);

    bt->btkey = btkey;
    bt->btdat = btdat;
    return bt;
}

/*
 * B-Treeファイルを新規に作成します。
 * B-Treeで管理されるデータファイルも作成されます。
 * ファイル名には拡張子なしのベース名を指定します。
 * ファイル名には必要に応じてパスを指定できます。
 *
 * キーサイズは最大1024バイトに制限されています。
 *
 * filename: ファイル名のポインタ
 * keysize: キーのバイト数
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int btcreate(const char* filename, int keysize)
{
    char fpath[MAX_PATH+1];
    ushort pagesize;
    ushort order;

    if (strlen(filename)+4 > MAX_PATH) {
        err_write("btcreate: filename is too long.");
        return -1;
    }
    if (keysize > MAX_KEYSIZE) {
        err_write("btcreate: key size is too large.");
        return -1;
    }

    /* キーサイズからページサイズを決定します。*/
    if (keysize > 0 && keysize < 32)
        pagesize = 4096;
    else if (keysize > 32 && keysize < 64)
        pagesize = 8192;
    else if (keysize > 64 && keysize < 128)
        pagesize = 16 * 1024;
    else
        pagesize = 32 * 1024;

    /* データ識別コード（2バイト固定文字）+
       キー数（2バイト）+
       次レベルページポインタ（4バイト）= 8 */

    /* キー（キーサイズ）+
       データページポインタ（8バイト）+
       次レベルページポインタ（4バイト）*/

    /* ページサイズから次数を決定します。*/
    order = (unsigned short)((pagesize - 8) / (keysize + 12));

    /* キーファイルを新規に作成します。*/
    sprintf(fpath, "%s%s", filename, KEY_FILE_EXT);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    if (btk_create(fpath, pagesize, (ushort)keysize, order) < 0)
        return -1;

    /* データファイルを新規に作成します。*/
    sprintf(fpath, "%s%s", filename, DATA_FILE_EXT);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    if (dio_create(fpath) < 0)
        return -1;
    return 0;
}

/*
 * B-Treeファイルをクローズ作成します。
 * 内部で確保されていたメモリは解放されます。
 * クローズ後はファイルへのアクセスは行なえません。
 *
 * bt: B-Treeの構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void btclose(struct btree_t* bt)
{
    btk_close(bt->btkey);
    dio_close(bt->btdat);

    /* クリティカルセクションの削除 */
    CS_DELETE(&bt->critical_section);

    free(bt);
}

/*
 * B-Treeファイルが存在するか調べます。
 * ファイル名には拡張子なしのベース名を指定します。
 * ファイル名には必要に応じてパスを指定できます。
 *
 * filename: ファイル名のポインタ
 *
 * 戻り値
 *  B-Treeファイルが存在する場合は 1 を返します。
 *  存在しない場合はゼロを返します。
 */
APIEXPORT int btfile(const char* filename)
{
    char fpath[MAX_PATH+1];
    struct stat file_stat;

    if (strlen(filename)+4 > MAX_PATH) {
        err_write("btfile: filename is too long.");
        return 0;
    }
    sprintf(fpath, "%s%s", filename, KEY_FILE_EXT);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    /* ファイル情報の取得 */
    if (stat(fpath, &file_stat) < 0)
        return 0;

    /* ディレクトリか調べます。*/
    if (S_ISDIR(file_stat.st_mode))
        return 0;
    /* ファイルが存在する */
    return 1;
}

/* キーサイズを調整します。
 * 小さい場合はゼロをパディングして akey に設定します。
 */
static void adjust_key(struct btkey_t* btkey,
                       const void* key,
                       int keysize,
                       void* akey)
{
    if (keysize == btkey->keysize) {
        memcpy(akey, key, btkey->keysize);
        return;
    }
    memset(akey, '\0', btkey->keysize);
    memcpy(akey, key, keysize);
}

/* B-Treeのルートページを作成します。
 */
static int create_root(struct btkey_t* btkey,
                       const void* key,
                       long dataptr,
                       int left,
                       int right)
{
    int result = 0;
    int rpn;
    struct btk_page_t* page;

    /* ファイル上のページ領域を取得します。*/
    rpn = btk_avail_page(btkey);
    if (rpn < 0)
        return -1;
    /* ページメモリを確保します。*/
    page = btk_alloc_page(btkey);
    if (page == NULL)
        return -1;

    /* キーとデータをセットします。*/
    memcpy(page->keytbl[0].key, key, btkey->keysize);
    page->keytbl[0].dataptr = dataptr;
    page->child[0] = left;
    page->child[1] = right;
    page->keycount = 1;

    /* ファイルへ書き出します。*/
    result = btk_write_page(btkey, rpn, page);

    /* メモリ領域を開放します。*/
    btk_free_page(page);

    /* ルートポインタを更新します。*/
    if (result == 0)
        result = btk_put_root(btkey, rpn);
    return result;
}

static int binary_search_node(struct btk_page_t* page,
                              const void* key,
                              int keysize,
                              int start,
                              int end,
                              int* index)
{
    int cmp = 0;
    int count;
    int mid;

    while(1) {
        count = end - start + 1;
        if (count <= 2) {
            int i;

            for (i = start; i <= end && (cmp = memcmp(key, page->keytbl[i].key, keysize)) > 0; i++)
                ;

            *index = i;
            if (i < page->keycount && cmp == 0)
                return 1;   /* キーはノードにある */
            else
                return 0;   /* キーはノードにない */
        }
        mid = start + count / 2;
        cmp = memcmp(key, page->keytbl[mid].key, keysize);
        if (cmp > 0)
            start = mid + 1;
        else if (cmp < 0)
            end = mid;
        else {
            *index = mid;
            return 1;   /* キーはノードにある */
        }
    }
}

/* ページからキー値を検索します。
 * 等しいか大きい位置を index に返します。
 */
static int search_node(struct btk_page_t* page,
                       const void* key,
                       int keysize,
                       int* index)
{
    int cmp;
    /* 右端をチェックします。*/
    cmp = memcmp(key, page->keytbl[page->keycount-1].key, keysize);
    if (cmp > 0) {
        *index = page->keycount;
        return 0;   /* キーはノードにない */
    } else if (cmp == 0) {
        *index = page->keycount - 1;
        return 1;   /* キーはノードにある */
    }

    /* 左端をチェックします。*/
    cmp = memcmp(key, page->keytbl[0].key, keysize);
    if (cmp < 0) {
        *index = 0;
        return 0;   /* キーはノードにない */
    } else if (cmp == 0) {
        *index = 0;
        return 1;   /* キーはノードにある */
    }

    /* ２分探索でノード内を検索します。*/
    return binary_search_node(page,
                              key,
                              keysize,
                              0,
                              page->keycount,
                              index);
}

/* キーを検索してデータのポインタを返します。
 * キーが存在しない場合は -1 を返します。
 * キーが存在してもデータがない場合はゼロを返します。
 */
static long find_key(struct btkey_t* btkey,
                     const void* key,
                     int* node_rpn,
                     int* node_index)
{
    long dataptr = -1;
    int rpn;

    rpn = btkey->root;
    while (rpn != 0) {
        int index;

        if (btk_read_page(btkey, rpn, btkey->wkpage) != 0)
            break;  /* error */
        if (search_node(btkey->wkpage, key, btkey->keysize, &index)) {
            dataptr = btkey->wkpage->keytbl[index].dataptr;
            if (node_rpn != NULL)
                *node_rpn = rpn;
            if (node_index != NULL)
                *node_index = index;
            break;  /* found */
        }
        rpn = btkey->wkpage->child[index];
    }
    return dataptr;
}

static void ins_in_page(struct btk_page_t* page,
                        const void* key,
                        int keysize,
                        long dataptr,
                        int r_child)
{
    int i;

    for (i = page->keycount; i > 0; i--) {
        int cmp;

        cmp = memcmp(key, page->keytbl[i-1].key, keysize);
        if (cmp >= 0)
            break;
        memcpy(page->keytbl[i].key, page->keytbl[i-1].key, keysize);
        page->keytbl[i].dataptr = page->keytbl[i-1].dataptr;
        page->child[i+1] = page->child[i];
    }
    page->keycount++;
    memcpy(page->keytbl[i].key, key, keysize);
    page->keytbl[i].dataptr = dataptr;
    page->child[i+1] = r_child;
}

static void split_page(struct btkey_t* btkey,
                       const void* key,
                       long dataptr,
                       int r_child,
                       struct btk_page_t* page,
                       void* promo_key,
                       long* promo_dataptr,
                       int* promo_r_child,
                       struct btk_page_t* newpage)
{
    int i, j;
    int mid, newcnt;

    /* 昇進させる位置を決定 */
    *promo_r_child = btk_avail_page(btkey);
    mid = btkey->order / 2 + btkey->order % 2;

    /* 後半を新しいページに移す */
    newcnt = btkey->order - mid;
    j = mid + 1;
    for (i = 0; i < newcnt; i++, j++) {
        memcpy(newpage->keytbl[i].key, page->keytbl[j].key, btkey->keysize);
        newpage->keytbl[i].dataptr = page->keytbl[j].dataptr;
        newpage->child[i] = page->child[j];
    }
    newpage->child[i] = page->child[j];
    newpage->keycount = newcnt;

    /* 真ん中のキーを昇進させる */
    memcpy(promo_key, page->keytbl[mid].key, btkey->keysize);
    *promo_dataptr = page->keytbl[mid].dataptr;

    /* 後半へ移動させた領域をクリアする */
    for (i = mid; i < btkey->order+1; i++) {
        memset(page->keytbl[i].key, '\0', btkey->keysize);
        page->keytbl[i].dataptr = 0;
        page->child[i+1] = 0;
    }
    page->keycount = mid;
}

static int insert_key(struct btkey_t* btkey,
                      int rpn,
                      const void* key,
                      long dataptr,
                      int* promo_r_child,
                      void* promo_key,
                      long* promo_dataptr)
{
    struct btk_page_t* page;
    int found;
    int promoted;
    int index;
    int p_b_rpn;
    void* p_b_key;
    long p_b_dataptr;

    if (rpn == 0) {
        memcpy(promo_key, key, btkey->keysize);
        *promo_dataptr = dataptr;
        *promo_r_child = 0;
        return 1;
    }
    /* ページメモリを確保します。*/
    page = btk_alloc_page(btkey);
    if (page == NULL)
        return 0;

    if (btk_read_page(btkey, rpn, page) != 0) {
        btk_free_page(page);
        return 0;   /* error */
    }
    found = search_node(page, key, btkey->keysize, &index);
    if (found) {
        err_write("insert_key: attempt to insert duplicate key.");
        btk_free_page(page);
        return 0;
    }
    p_b_key = alloca(btkey->keysize);
    promoted = insert_key(btkey,
                          page->child[index],
                          key,
                          dataptr,
                          &p_b_rpn,
                          p_b_key,
                          &p_b_dataptr);
    if (! promoted) {
        btk_free_page(page);
        return 0;
    }

    /* ページ分割のために order + 1 分の領域が確保されている。*/
    if (page->keycount < btkey->order+1) {
        /* ページ内に挿入 */
        ins_in_page(page, p_b_key, btkey->keysize, p_b_dataptr, p_b_rpn);
        if (page->keycount <= btkey->order) {
            btk_write_page(btkey, rpn, page);
            btk_free_page(page);
            return 0;
        } else {
            /* ページを２分割します。*/
            btk_clear_page(btkey, btkey->wkpage);

            split_page(btkey,
                       p_b_key,
                       p_b_dataptr,
                       p_b_rpn,
                       page,
                       promo_key,
                       promo_dataptr,
                       promo_r_child,
                       btkey->wkpage);
            btk_write_page(btkey, rpn, page);
            btk_write_page(btkey, *promo_r_child, btkey->wkpage);
            btk_free_page(page);
            return 1;   /* 昇進 */
        }
    } else {
        err_write("insert_key: key count over.");
        btk_free_page(page);
        return 0;
    }
}

static int update_dataptr(struct btkey_t* btkey,
                          const void* key,
                          long dataptr)
{
    int result = -1;
    int rpn;

    rpn = btkey->root;
    while (rpn != 0) {
        int index;

        if (btk_read_page(btkey, rpn, btkey->wkpage) != 0)
            break;  /* error */

        if (search_node(btkey->wkpage, key, btkey->keysize, &index)) {
            /* データポインタを更新します。*/
            btkey->wkpage->keytbl[index].dataptr = dataptr;
            btk_write_page(btkey, rpn, btkey->wkpage);
            result = 0;
            break;
        }
        rpn = btkey->wkpage->child[index];
    }
    return result;
}

/*
 * B-Treeファイルへキー値とデータ値を書き出します。
 * キー値が存在していた場合はデータ値が更新されます。
 * キー値が存在しない場合はデータ値とキー値が挿入されます。
 *
 * bt: B-Treeの構造体のポインタ
 * key: キー値のポインタ
 * keysize: キー値のサイズ（バイト数）
 * val: データ値のポインタ
 * valsize: データ値のサイズ（バイト数）
 *
 * 戻り値
 *  正常に処理された場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int btput(struct btree_t* bt, const void* key, int keysize, const void* val, int valsize)
{
    int result = 0;
    void* akey;
    long dataptr;

    if (keysize > bt->btkey->keysize) {
        err_write("btput: keysize is too long.");
        return -1;
    }

    CS_START(&bt->critical_section);

    /* キーサイズを調整します。*/
    akey = alloca(bt->btkey->keysize);
    adjust_key(bt->btkey, key, keysize, akey);

    if (bt->btkey->root == 0L) {
        /* データを書き出します。*/
        dataptr = 0;
        if (val != NULL) {
            dataptr = dio_avail_space(bt->btdat, valsize);
            if (dataptr > 0)
                result = dio_write(bt->btdat, dataptr, val, valsize);
        }
        if (result == 0) {
            /* ルートを作成します。*/
            result = create_root(bt->btkey, akey, dataptr, 0, 0);
        }
    } else {
        /* キーを検索します。*/
        dataptr = find_key(bt->btkey, akey, NULL, NULL);
        if (dataptr < 0) {
            int promo_r_child;
            void* promo_key;
            long promo_dataptr;
            int promoted;

            /* データを新規に書き出します。*/
            dataptr = 0;
            if (val != NULL) {
                dataptr = dio_avail_space(bt->btdat, valsize);
                if (dataptr > 0)
                    result = dio_write(bt->btdat, dataptr, val, valsize);
            }
            if (result == 0) {
                /* キーを新規に挿入します。*/
                promo_key = alloca(bt->btkey->keysize);
                promoted = insert_key(bt->btkey,
                                      bt->btkey->root,
                                      akey,
                                      dataptr,
                                      &promo_r_child,
                                      promo_key,
                                      &promo_dataptr);
                if (promoted) {
                    /* 昇進したキーをルートに設定します。*/
                    result = create_root(bt->btkey,
                                         promo_key,
                                         promo_dataptr,
                                         bt->btkey->root,
                                         promo_r_child);
                }
            }
        } else if (dataptr > 0) {
            int areasize;

            /* 元の領域サイズを調べます。*/
            areasize = dio_area_size(bt->btdat, dataptr);
            if (areasize >= valsize) {
                /* データを置換します。*/
                result = dio_write(bt->btdat, dataptr, val, valsize);
            } else {
                long newptr;

                /* 元の位置に収まらないので新たな領域に書き込みます。*/
                newptr = dio_avail_space(bt->btdat, valsize);
                if (newptr < 0)
                    result = -1;
                else
                    result = dio_write(bt->btdat, newptr, val, valsize);
                /* 元の領域を削除します。*/
                dio_delete(bt->btdat, dataptr);

                /* キーのデータを更新します。*/
                result = update_dataptr(bt->btkey, akey, newptr);
            }
        } else {
            if (val != NULL) {
                long newptr;

                /* キーのみの状態からデータが追加になった */
                newptr = dio_avail_space(bt->btdat, valsize);
                if (newptr < 0)
                    result = -1;
                else
                    result = dio_write(bt->btdat, newptr, val, valsize);
                /* キーのデータを更新します。*/
                result = update_dataptr(bt->btkey, akey, newptr);
            }
        }
    }
    CS_END(&bt->critical_section);
    return result;
}

/*
 * B-Treeファイルからキー値を検索してデータ値のサイズを取得します。
 *
 * bt: B-Treeの構造体のポインタ
 * key: キー値のポインタ
 * keysize: キー値のサイズ（バイト数）
 *
 * 戻り値
 *  正常に処理された場合はデータのサイズ（バイト数）を返します。
 *  データがない場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int btsearch(struct btree_t* bt, const void* key, int keysize)
{
    int dsize = 0;
    void* akey;
    long dataptr;

    if (keysize > bt->btkey->keysize) {
        err_write("btsearch: keysize is too long.");
        return -1;
    }

    CS_START(&bt->critical_section);

    /* キーサイズを調整します。*/
    akey = alloca(bt->btkey->keysize);
    adjust_key(bt->btkey, key, keysize, akey);

    dataptr = find_key(bt->btkey, akey, NULL, NULL);
    if (dataptr > 0) {
        /* データサイズを取得します。*/
        dsize = dio_data_size(bt->btdat, dataptr);
    } else if (dataptr < 0) {
        /* キーが存在しない */
        dsize = -1;
    }
    CS_END(&bt->critical_section);
    return dsize;
}

/*
 * B-Treeファイルからキー値を検索してデータ値を取得します。
 * データ値が設定される領域 val は呼び出し側で確保しておきます。
 * データ値のサイズ valsize が小さい場合はエラーになります。
 *
 * bt: B-Treeの構造体のポインタ
 * key: キー値のポインタ
 * keysize: キー値のサイズ（バイト数）
 * val: データ値のポインタ
 * valsize: データ値のサイズ（バイト数）
 *
 * 戻り値
 *  正常に処理された場合はデータ値のバイト数を返します。
 *  エラーの場合は -1 を返します。
 *  データ値のサイズが足らない場合は -2 を返します。
 */
APIEXPORT int btget(struct btree_t* bt, const void* key, int keysize, void* val, int valsize)
{
    int dsize = -1;
    void* akey;
    long dataptr;

    if (keysize > bt->btkey->keysize) {
        err_write("btget: keysize is too long.");
        return -1;
    }

    CS_START(&bt->critical_section);

    /* キーサイズを調整します。*/
    akey = alloca(bt->btkey->keysize);
    adjust_key(bt->btkey, key, keysize, akey);

    dataptr = find_key(bt->btkey, akey, NULL, NULL);
    if (dataptr > 0) {
        int result;

        /* データサイズを取得します。*/
        dsize = dio_data_size(bt->btdat, dataptr);
        if (dsize > 0) {
           if (valsize >= dsize) {
                /* データを取得します。*/
                result = dio_read(bt->btdat, dataptr, val, dsize);
                if (result != 0)
                    dsize = -1;
            } else {
                /* 領域不足 */
                dsize = -2;
            }
        }
    } else if (dataptr == 0) {
        /* データなし */
        dsize = 0;
    }
    CS_END(&bt->critical_section);
    return dsize;
}

static int get_leaf_page(struct btkey_t* btkey,
                         int rpn,
                         struct btk_page_t* page)
{
    int node_rpn = 0;
    int p;

    for (p = rpn; p != 0; ) {
        if (btk_read_page(btkey, p, page) != 0)
            return -1;
        node_rpn = p;
        p = page->child[0];
    }
    return node_rpn;
}

static void swap_key(struct btkey_t* btkey,
                     struct btk_page_t* pg1, int pos1,
                     struct btk_page_t* pg2, int pos2)
{
    void* tkey;
    long tdptr;

    tkey = alloca(btkey->keysize);

    /* pg1 -> temp */
    memcpy(tkey, pg1->keytbl[pos1].key, btkey->keysize);
    tdptr = pg1->keytbl[pos1].dataptr;

    /* pg2 -> pg1 */
    memcpy(pg1->keytbl[pos1].key, pg2->keytbl[pos2].key, btkey->keysize);
    pg1->keytbl[pos1].dataptr = pg2->keytbl[pos2].dataptr;

    /* temp -> pg2 */
    memcpy(pg2->keytbl[pos2].key, tkey, btkey->keysize);
    pg2->keytbl[pos2].dataptr = tdptr;
}

static void delete_page_key(struct btkey_t* btkey,
                            struct btk_page_t* page,
                            int index)
{
    if (index+1 < page->keycount) {
        int i;

        for (i = index+1; i < page->keycount; i++) {
            memcpy(page->keytbl[i-1].key, page->keytbl[i].key, btkey->keysize);
            page->keytbl[i-1].dataptr = page->keytbl[i].dataptr;
        }
    }
    page->keycount--;
    if (page->keycount > 0) {
        /* 親ノードを検索する場合に参照されるためすべてはクリアしません。*/
        memset(page->keytbl[page->keycount].key, '\0', btkey->keysize);
    }
    page->keytbl[page->keycount].dataptr = 0;
}

static int search_child(struct btk_page_t* page, int target_rpn)
{
    int i;

    for (i = 0; i < page->keycount+1; i++) {
        if (page->child[i] == target_rpn)
            return i;
    }
    return -1;
}

/* 親ノードのページ番号を返します。*/
static int search_parent_node(struct btkey_t* btkey,
                              const void* key,
                              int target_rpn,
                              int* rpos,
                              int* right_node_flag)
{
    int p_rpn = 0;
    int rpn;
    struct btk_page_t* page;

    /* ページメモリを確保します。*/
    page = btk_alloc_page(btkey);
    if (page == NULL)
        return -1;

    *right_node_flag = 0;

    rpn = btkey->root;
    while (rpn != 0) {
        int index;

        if (btk_read_page(btkey, rpn, page) != 0)
            break;  /* error */

        p_rpn = rpn;

        /* 子孫ポインタと一致するか？ */
        index = search_child(page, target_rpn);
        if (index >= 0) {
            /* 親が見つかった。*/
            if (index == page->keycount) {
                /* 右端 */
                *right_node_flag = 1;
            }
            *rpos = index;
            break;
        }

        /* 次に調べる子孫ノードを決めます。*/
        if (search_node(page, key, btkey->keysize, &index)) {
            /* found */
            if (index == page->keycount) {
                /* 右端 */
                *right_node_flag = 1;
            }
            *rpos = index;
            break;
        }
        rpn = page->child[index];
    }

    /* メモリ領域を開放します。*/
    btk_free_page(page);
    return p_rpn;
}

/* ノードの内容を入れ替えます。*/
static void swap_node(struct btkey_t* btkey,
                      int* rpn1, struct btk_page_t* p1,
                      int* rpn2, struct btk_page_t* p2)
{
    struct btk_page_t* tp;
    int t_rpn;

    tp = btk_alloc_page(btkey);
    if (tp == NULL)
        return;

    btk_page_copy(btkey, tp, p1);
    t_rpn = *rpn1;
    btk_page_copy(btkey, p1, p2);
    *rpn1 = *rpn2;
    btk_page_copy(btkey, p2, tp);
    *rpn2 = t_rpn;

    btk_free_page(tp);
}

/* 過疎状態になっているページを連結して１ページに収めます。*/
static void cat_node(struct btkey_t* btkey,
                     struct btk_page_t* page,
                     struct btk_page_t* p_page,
                     int p_pos,
                     struct btk_page_t* s_page)
{
    void* key;
    long dataptr;
    int cnt;
    int p_cnt;
    int s_cnt;
    int i;

    key = alloca(btkey->keysize);

    /* 親のキーをページの最後に追加します。
       子孫ポインターはコピーしません。*/
    memcpy(key, p_page->keytbl[p_pos].key, btkey->keysize);
    dataptr = p_page->keytbl[p_pos].dataptr;

    cnt = page->keycount;
    memcpy(page->keytbl[cnt].key, key, btkey->keysize);
    page->keytbl[cnt].dataptr = dataptr;
    page->keycount++;

    /* ページに追加した親のキーを削除します。
       子孫ポインターもずらします。*/
    p_cnt = p_page->keycount;
    delete_page_key(btkey, p_page, p_pos);
    for (i = p_pos+1; i < p_cnt; i++)
        p_page->child[i] = p_page->child[i+1];
    p_page->child[p_cnt] = 0;

    /* 兄弟のキー＆子孫ポインターをすべて追加します。*/
    cnt = page->keycount;
    s_cnt = s_page->keycount;
    for (i = 0; i < s_cnt; i++) {
        memcpy(key, s_page->keytbl[i].key, btkey->keysize);
        dataptr = s_page->keytbl[i].dataptr;
        memcpy(page->keytbl[i+cnt].key, key, btkey->keysize);
        page->keytbl[i+cnt].dataptr = dataptr;
        page->child[i+cnt] = s_page->child[i];
    }
    page->child[s_cnt+cnt] = s_page->child[s_cnt];
    /* キー件数を設定します。*/
    page->keycount += s_cnt;
}

/* 過疎状態になっているページを再配分します。*/
static void redist_node(struct btkey_t* btkey,
                        struct btk_page_t* page,
                        struct btk_page_t* p_page,
                        int p_pos,
                        struct btk_page_t* s_page)
{
    void* wkey;
    long* wdataptr;
    int* wchild;
    int wcnt;
    int i, j;
    char* p;
    int mid;

    /* ワーク領域を確保する（2ページ分+親の分の大きさを確保） */
    wkey = malloc((btkey->order*2+1) * btkey->keysize);
    if (wkey == NULL) {
        err_write("redist_node: no memory.");
        return;
    }
    wdataptr = (long*)malloc((btkey->order*2+1) * sizeof(long));
    if (wdataptr == NULL) {
        err_write("redist_node: no memory.");
        free(wkey);
        return;
    }
    wchild = (int*)malloc((btkey->order*2+2) * sizeof(int));
    if (wchild == NULL) {
        err_write("redist_node: no memory.");
        free(wdataptr);
        free(wkey);
        return;
    }

    /* ページの内容をワーク領域にコピーします。*/
    p = (char*)wkey;
    for (i = 0; i < page->keycount; i++) {
        memcpy(p, page->keytbl[i].key, btkey->keysize);
        wdataptr[i] = page->keytbl[i].dataptr;
        wchild[i] = page->child[i];
        p += btkey->keysize;
    }
    wchild[page->keycount] = page->child[page->keycount];
    wcnt = page->keycount;

    /* 親のキーをワーク領域にコピーします。*/
    memcpy(p, p_page->keytbl[p_pos].key, btkey->keysize);
    wdataptr[wcnt] = p_page->keytbl[p_pos].dataptr;
    p += btkey->keysize;
    wcnt++;

    /* 兄弟の内容をすべてワーク領域にコピーします。*/
    for (i = 0; i < s_page->keycount; i++) {
        memcpy(p, s_page->keytbl[i].key, btkey->keysize);
        wdataptr[i+wcnt] = s_page->keytbl[i].dataptr;
        wchild[i+wcnt] = s_page->child[i];
        p += btkey->keysize;
    }
    wchild[s_page->keycount+wcnt] = s_page->child[s_page->keycount];
    /* ワーク領域のキー件数を設定します。*/
    wcnt += s_page->keycount;

    /* 中心のキーを親へ移します。*/
    mid = wcnt / 2;
    p = (char*)wkey + (mid * btkey->keysize);
    memcpy(p_page->keytbl[p_pos].key, p, btkey->keysize);
    p_page->keytbl[p_pos].dataptr = wdataptr[mid];

    /* 前半を左ページへ移します。*/
    btk_clear_page(btkey, page);
    p = (char*)wkey;
    for (i = 0; i < mid; i++) {
        memcpy(page->keytbl[i].key, p, btkey->keysize);
        page->keytbl[i].dataptr = wdataptr[i];
        page->child[i] = wchild[i];
        p += btkey->keysize;
    }
    page->child[mid] = wchild[mid];
    page->keycount = mid;

    /* 後半を右ページへ移します。*/
    btk_clear_page(btkey, s_page);
    s_page->keycount = wcnt - mid - 1;
    p = (char*)wkey + (mid + 1) * btkey->keysize;
    for (i = 0, j = mid + 1; i < s_page->keycount; i++, j++) {
        memcpy(s_page->keytbl[i].key, p, btkey->keysize);
        s_page->keytbl[i].dataptr = wdataptr[j];
        s_page->child[i] = wchild[j];
        p += btkey->keysize;
    }
    s_page->child[s_page->keycount] = wchild[wcnt];

    free(wchild);
    free(wdataptr);
    free(wkey);
}

static int adjust_node(struct btkey_t* btkey,
                       int rpn,
                       struct btk_page_t* page)
{
    struct btk_page_t* p_page;
    struct btk_page_t* s_page;
    int p_rpn;
    int s_rpn;
    int p_child_index = 0;
    int p_pos;
    int right_node_flag = 0;
    int index;
    int kc;
    int result = 0;

    if (page->keycount > btkey->order/2) {
        /* 最小キー数より多いので調整の必要なし。*/
        return btk_write_page(btkey, rpn, page);
    }

    /* 過疎状態になっている。*/
    if (rpn == btkey->root) {
        /* ルート */
        if (page->keycount < 1) {
            /* キーが存在しなくなった。*/
            if (btk_delete_page(btkey, rpn) < 0)
                return -1;
            return btk_put_root(btkey, 0);
        }
        return btk_write_page(btkey, rpn, page);
    }

    /* 親を取得します。*/
    p_rpn = search_parent_node(btkey, page->keytbl[0].key, rpn, &p_child_index, &right_node_flag);
    if (p_rpn < 0)
        return -1;
    if (p_rpn == 0)
        return btk_write_page(btkey, rpn, page);

    /* 親のページメモリを確保します。*/
    p_page = btk_alloc_page(btkey);
    if (p_page == NULL)
        return -1;

    /* 親のページを読みます。*/
    if (btk_read_page(btkey, p_rpn, p_page) < 0) {
        btk_free_page(p_page);
        return -1;
    }

    /* 兄弟(sibling)のページを取得します。*/
    s_page = btk_alloc_page(btkey);
    if (s_page == NULL) {
        btk_free_page(p_page);
        return -1;
    }
    if (right_node_flag) {
        p_pos = p_child_index - 1;
        index = p_child_index - 1;
    } else {
        p_pos = p_child_index;
        index = p_child_index + 1;
    }
    s_rpn = p_page->child[index];
    if (btk_read_page(btkey, s_rpn, s_page) < 0) {
        btk_free_page(s_page);
        btk_free_page(p_page);
        return -1;
    }
    if (right_node_flag) {
        /* 対象が右端なのでノードの内容を入れ替えます。*/
        swap_node(btkey, &rpn, page, &s_rpn, s_page);
    }

    kc = page->keycount + s_page->keycount;
    if (kc < btkey->order) {
        /* 親のキーもノードに追加されるため
           オーダー数より少ない場合にノードを連結します。*/
        cat_node(btkey, page, p_page, p_pos, s_page);
        if (btk_write_page(btkey, rpn, page) < 0) {
            btk_free_page(s_page);
            btk_free_page(p_page);
            return -1;
        }
        /* 兄弟(s_page)を削除します。*/
        if (btk_delete_page(btkey, s_rpn) < 0) {
            btk_free_page(s_page);
            btk_free_page(p_page);
            return -1;
        }
        if (p_page->keycount < 1) {
            /* 親も削除します。*/
            if (p_rpn == btkey->root) {
                if (btk_delete_page(btkey, p_rpn) < 0) {
                    btk_free_page(s_page);
                    btk_free_page(p_page);
                    return -1;
                }
                /* ルートを更新します。*/
                if (btk_put_root(btkey, rpn) < 0) {
                    btk_free_page(s_page);
                    btk_free_page(p_page);
                    return -1;
                }
                p_rpn = 0;
            }
        }
        if (p_rpn != 0) {
            /* 親ノードを再帰で処理します。*/
            result = adjust_node(btkey, p_rpn, p_page);
        }
        btk_free_page(s_page);
        btk_free_page(p_page);
        return result;
    }

    /* 再配分します。*/
    redist_node(btkey, page, p_page, p_pos, s_page);

    /* 各ページを書き出します。*/
    result = btk_write_page(btkey, p_rpn, p_page);
    if (result == 0)
        result = btk_write_page(btkey, s_rpn, s_page);
    if (result == 0)
        result = btk_write_page(btkey, rpn, page);

    btk_free_page(s_page);
    btk_free_page(p_page);
    return result;
}

static int delete_keypos(struct btkey_t* btkey,
                         int rpn,
                         int pos)
{
    int result = 0;
    int child;

    /* 削除対象ノードの読み込み */
    if (btk_read_page(btkey, rpn, btkey->wkpage) < 0)
        return -1;

    child = btkey->wkpage->child[pos+1];
    if (child == 0) {
        /* リーフなのでそのまま削除 */
        delete_page_key(btkey, btkey->wkpage, pos);
        adjust_node(btkey, rpn, btkey->wkpage);
    } else {
        struct btk_page_t* leaf_page;
        int leaf_rpn;

        /* リーフのページと入れ換えてから削除 */
        leaf_page = btk_alloc_page(btkey);
        if (leaf_page == NULL)
            return -1;

        /* リーフページを取得します。*/
        leaf_rpn = get_leaf_page(btkey, child, leaf_page);
        if (leaf_rpn < 0) {
            result = -1;
        } else {
            /* キーを入れ替えます。*/
            swap_key(btkey, btkey->wkpage, pos, leaf_page, 0);
            result = btk_write_page(btkey, rpn, btkey->wkpage);
            if (result == 0) {
                delete_page_key(btkey, leaf_page, 0);
                adjust_node(btkey, leaf_rpn, leaf_page);
            }
        }
        btk_free_page(leaf_page);
    }
    return result;
}

/*
 * B-Treeファイルからキー値を検索して削除します。
 * キーとデータが削除されます。
 *
 * bt: B-Treeの構造体のポインタ
 * key: キー値のポインタ
 * keysize: キー値のサイズ（バイト数）
 *
 * 戻り値
 *  正常に処理された場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int btdelete(struct btree_t* bt, const void* key, int keysize)
{
    int result = -1;
    void* akey;
    long dataptr;
    int rpn;
    int pos;

    if (keysize > bt->btkey->keysize) {
        err_write("btdelete: keysize is too long.");
        return -1;
    }

    CS_START(&bt->critical_section);

    /* キーサイズを調整します。*/
    akey = alloca(bt->btkey->keysize);
    adjust_key(bt->btkey, key, keysize, akey);

    dataptr = find_key(bt->btkey, akey, &rpn, &pos);
    if (dataptr > 0) {
        /* キーを削除します。*/
        result = delete_keypos(bt->btkey, rpn, pos);
        if (result == 0) {
            /* データを削除します。*/
            dio_delete(bt->btdat, dataptr);
        }
    }
    CS_END(&bt->critical_section);
    return result;
}
