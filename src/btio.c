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

#define BT_FILE_VERSION         10

#define KEY_HEADER_SIZE         64

#define KEY_VERSION_OFFSET      4
#define KEY_TIMESTAMP_OFFSET    6
#define KEY_PAGESIZE_OFFSET     14
#define KEY_KEYSIZE_OFFSET      16
#define KEY_ORDER_OFFSET        18
#define KEY_ROOT_OFFSET         20
#define KEY_FREE_OFFSET         24
#define KEY_FILEID              "NKVK"
#define KEY_PAGEID              0xAAEE
#define KEY_FREE_PAGEID         0xBBEE

#define KEY_PAGE_OFFSET(rpn,psize) (KEY_HEADER_SIZE+((rpn-1)*psize))
#define KEY_PAGE_NO(offset,psize)  ((offset-KEY_HEADER_SIZE)/psize+1)

static int page_memsize(struct btkey_t* btkey)
{
    int msize;

    /* split_node() のために order + 1 で確保します。*/
    msize = sizeof(struct btk_page_t) +
            (btkey->order+1) * sizeof(struct btk_element_t) +
            (btkey->order+1) * btkey->keysize +
            (btkey->order+2) * sizeof(int);
    return msize;
}

struct btkey_t* btk_open(const char* fname, int cache_size)
{
    int fd;
    struct btkey_t* btkey;
    char buf[KEY_HEADER_SIZE];
    char fid[4];
    ushort fver;
    int64 ctime;
    ushort pagesize;
    ushort keysize;
    ushort order;
    int key_cache_count = 0;

    fd = FILE_OPEN(fname, O_RDWR|O_BINARY);
    if (fd < 0) {
        err_write("btk_open: file can't open: %s.", fname);
        return NULL;
    }
    btkey = (struct btkey_t*)calloc(1, sizeof(struct btkey_t));
    if (btkey == NULL) {
        err_write("btk_open: no memory.");
        FILE_CLOSE(fd);
        return NULL;
    }

    btkey->fd = fd;

    /* メモリマップドファイルのオープン */
    btkey->mmap = mmap_open(fd, MMAP_READWRITE, MMAP_AUTO_SIZE);
    if (btkey->mmap == NULL) {
        err_write("btk_open: can't open mmap.");
        free(btkey);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* ヘッダー部の読み込み */
    if (mmap_read(btkey->mmap, buf, KEY_HEADER_SIZE) != KEY_HEADER_SIZE) {
        err_write("btk_open: can't read header.");
        mmap_close(btkey->mmap);
        free(btkey);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* ファイル識別コード */
    memcpy(fid, buf, sizeof(fid));
    if (memcmp(fid, KEY_FILEID, sizeof(fid)) != 0) {
        err_write("btk_open: illegal file.");
        mmap_close(btkey->mmap);
        free(btkey);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* ファイルバージョン（2バイト）*/
    memcpy(&fver, &buf[KEY_VERSION_OFFSET], sizeof(fver));

    /* 作成日時 */
    memcpy(&ctime, &buf[KEY_TIMESTAMP_OFFSET], sizeof(ctime));

    /* ページサイズ（2バイト）*/
    memcpy(&pagesize, &buf[KEY_PAGESIZE_OFFSET], sizeof(pagesize));

    /* キーサイズ（2バイト）*/
    memcpy(&keysize, &buf[KEY_KEYSIZE_OFFSET], sizeof(keysize));
    if (keysize > MAX_KEYSIZE) {
        err_write("btk_open: illegal key size.");
        mmap_close(btkey->mmap);
        free(btkey);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* 次数（2バイト）*/
    memcpy(&order, &buf[KEY_ORDER_OFFSET], sizeof(order));

    btkey->pagesize = pagesize;
    btkey->keysize = keysize;
    btkey->order = order;

    /* ルートページポインタ（4バイト）*/
    memcpy(&btkey->root, &buf[KEY_ROOT_OFFSET], sizeof(btkey->root));

    /* 空きページポインタ（4バイト）*/
    memcpy(&btkey->free, &buf[KEY_FREE_OFFSET], sizeof(btkey->free));

    /* キーページのメモリ領域サイズを求めます。*/
    btkey->page_memsize = page_memsize(btkey);
    if (cache_size > 0) {
        /* キャッシュサイズ(KB)からページ件数を求めます。*/
        key_cache_count = (cache_size * 1024) / btkey->page_memsize;
    }
    if (key_cache_count > 0) {
        /* ページキャッシュを作成します。*/
        btkey->page_cache = btk_cache_alloc(btkey, key_cache_count);
    }

    /* 作業用ページ領域の確保 */
    btkey->wkpage = btk_alloc_page(btkey);
    if (btkey->wkpage == NULL) {
        err_write("btk_open: no memory, work page.");
        btk_cache_free(btkey->page_cache);
        mmap_close(btkey->mmap);
        free(btkey);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* ページread/write用の領域を確保 */
    btkey->pagebuf = malloc(btkey->pagesize);
    if (btkey->pagebuf == NULL) {
        err_write("btk_open: no memory, pagebuf.");
        btk_free_page(btkey->wkpage);
        btk_cache_free(btkey->page_cache);
        mmap_close(btkey->mmap);
        free(btkey);
        FILE_CLOSE(fd);
        return NULL;
    }
    return btkey;
}

int btk_create(const char* fname, ushort pagesize, ushort keysize, ushort order)
{
    int fd;
    char buf[KEY_HEADER_SIZE];
    ushort fver = BT_FILE_VERSION;
    int64 ctime;

    fd = FILE_OPEN(fname, O_RDWR|O_CREAT|O_BINARY, CREATE_MODE);
    if (fd < 0) {
        err_write("btk_create: file can't open: %s.", fname);
        return -1;
    }
    FILE_TRUNCATE(fd, 0);

    /* バッファのクリア */
    memset(buf, '\0', KEY_HEADER_SIZE);

    /* ファイル識別コード */
    memcpy(buf, KEY_FILEID, 4);

    /* ファイルバージョン（2バイト）*/
    memcpy(&buf[KEY_VERSION_OFFSET], &fver, sizeof(fver));

    /* 作成日時 */
    ctime = system_time();
    memcpy(&buf[KEY_TIMESTAMP_OFFSET], &ctime, sizeof(ctime));

    /* ページサイズ（2バイト）*/
    memcpy(&buf[KEY_PAGESIZE_OFFSET], &pagesize, sizeof(pagesize));

    /* キーサイズ（2バイト）*/
    memcpy(&buf[KEY_KEYSIZE_OFFSET], &keysize, sizeof(keysize));

    /* 次数（2バイト）*/
    memcpy(&buf[KEY_ORDER_OFFSET], &order, sizeof(order));

    /* ルートページポインタ（4バイト）*/
    /* 空きページポインタ（4バイト）*/

    /* ヘッダー部の書き出し */
    if (FILE_WRITE(fd, buf, KEY_HEADER_SIZE) != KEY_HEADER_SIZE) {
        err_write("btk_create: can't write header.");
        FILE_CLOSE(fd);
        return -1;
    }
    FILE_CLOSE(fd);
    return 0;
}

void btk_close(struct btkey_t* btkey)
{
    mmap_close(btkey->mmap);
    FILE_CLOSE(btkey->fd);
    btk_free_page(btkey->wkpage);
    btk_cache_free(btkey->page_cache);
    free(btkey->pagebuf);
    free(btkey);
}

int btk_put_root(struct btkey_t* btkey, int rpn)
{
    mmap_seek(btkey->mmap, KEY_ROOT_OFFSET);
    if (mmap_write(btkey->mmap, &rpn, sizeof(rpn)) != sizeof(rpn)) {
        err_write("btk_put_root: write error.");
        return -1;
    }
    btkey->root = rpn;
    return 0;
}

static int btk_put_free(struct btkey_t* btkey, int rpn)
{
    mmap_seek(btkey->mmap, KEY_FREE_OFFSET);
    if (mmap_write(btkey->mmap, &rpn, sizeof(rpn)) != sizeof(rpn)) {
        err_write("btk_put_free: write error.");
        return -1;
    }
    btkey->free = rpn;
    return 0;
}

int btk_avail_page(struct btkey_t* btkey)
{
    int rpn = -1;
    int64 ptr;

    if (btkey->free == 0) {
        ushort rid = KEY_PAGEID;

        /* 空き領域がないのでファイルの最後に追加します。*/
        ptr = btkey->mmap->real_size;
        memset(btkey->pagebuf, '\0', btkey->pagesize);
        memcpy(btkey->pagebuf, &rid, sizeof(rid));
        mmap_seek(btkey->mmap, ptr);
        if (mmap_write(btkey->mmap, btkey->pagebuf, btkey->pagesize) != btkey->pagesize) {
            err_write("btk_avail_page: write error.");
            return -1;
        }
        /* オフセットからページ番号を求めます。*/
        rpn = (int)KEY_PAGE_NO(ptr, btkey->pagesize);
    } else {
        ushort rid;
        int next_rpn;

        /* 空き領域を使用します。*/
        ptr = KEY_PAGE_OFFSET(btkey->free, btkey->pagesize);
        mmap_seek(btkey->mmap, ptr);
        if (mmap_read(btkey->mmap, &rid, sizeof(rid)) == sizeof(rid)) {
           if (rid == KEY_FREE_PAGEID) {
                if (mmap_read(btkey->mmap, &next_rpn, sizeof(next_rpn)) == sizeof(next_rpn)) {
                    rpn = btkey->free;
                    /* ヘッダー部の空きポインタを更新します。*/
                    if (btk_put_free(btkey, next_rpn) < 0) {
                        rpn = -1;
                    }
                } else {
                    err_write("btk_avail_page: read error.");
                }
            } else {
                err_write("btk_avail_page: illegal free page.");
            }
        } else {
            err_write("btk_avail_page: read error.");
        }
    }
    return rpn;
}

int btk_read_page(struct btkey_t* btkey, int rpn, struct btk_page_t* keypage)
{
    long ptr;
    ushort rid;
    ushort keycount;
    int i;
    char* p;

    if (rpn < 1) {
        err_write("btk_read: illegal rpn=%d.", rpn);
        return -1;
    }

    /* キャッシュから取得します。*/
    if (btk_cache_get(btkey->page_cache, rpn, keypage))
        return 0;

    ptr = KEY_PAGE_OFFSET(rpn, btkey->pagesize);
    mmap_seek(btkey->mmap, ptr);
    if (mmap_read(btkey->mmap, btkey->pagebuf, btkey->pagesize) != btkey->pagesize) {
        err_write("btk_read: read error.");
        return -1;
    }

    memcpy(&rid, btkey->pagebuf, sizeof(rid));
    if (rid != KEY_PAGEID) {
        err_write("btk_read: illegal rid.");
        return -1;
    }

    /* ページ領域クリア */
    btk_clear_page(btkey, keypage);

    /* キー数の設定 */
    memcpy(&keycount, &btkey->pagebuf[sizeof(rid)], sizeof(keycount));
    keypage->keycount = keycount;

    /* キー＆データポインタの設定 */
    p = &btkey->pagebuf[sizeof(rid)+sizeof(keycount)];
    for (i = 0; i < keypage->keycount; i++) {
        int64 dptr;

        memcpy(keypage->keytbl[i].key, p, btkey->keysize);
        p += btkey->keysize;
        memcpy(&dptr, p, sizeof(int64));
        keypage->keytbl[i].dataptr = (long)dptr;
        p += sizeof(int64);
    }
    /* 子孫ポインタの設定 */
    memcpy(keypage->child, p, (keypage->keycount+1) * sizeof(int));

    /* キャッシュに設定します。*/
    btk_cache_set(btkey->page_cache, rpn, keypage);
    return 0;
}

int btk_write_page(struct btkey_t* btkey, int rpn, struct btk_page_t* keypage)
{
    long ptr;
    ushort rid = KEY_PAGEID;
    ushort keycount;
    int i;
    char* p;

    if (rpn < 1) {
        err_write("btk_write: illegal rpn=%d.", rpn);
        return -1;
    }

    p = btkey->pagebuf;
    keycount = (ushort)keypage->keycount;
    memcpy(p, &rid, sizeof(rid));
    p += sizeof(rid);
    memcpy(p, &keycount, sizeof(keycount));
    p += sizeof(keycount);
    /* キー＆データポインタ */
    for (i = 0; i < keypage->keycount; i++) {
        int64 dptr;

        memcpy(p, keypage->keytbl[i].key, btkey->keysize);
        p += btkey->keysize;
        dptr = keypage->keytbl[i].dataptr;
        memcpy(p, &dptr, sizeof(int64));
        p += sizeof(int64);
    }

    /* 子孫ポインタ */
    memcpy(p, keypage->child, (keypage->keycount+1) * sizeof(int));

    ptr = KEY_PAGE_OFFSET(rpn, btkey->pagesize);
    mmap_seek(btkey->mmap, ptr);
    if (mmap_write(btkey->mmap, btkey->pagebuf, btkey->pagesize) != btkey->pagesize) {
        err_write("btk_write: write error.");
        return -1;
    }

    /* キャッシュにあれば内容を更新します。*/
    btk_cache_update(btkey->page_cache, rpn, keypage);
    return 0;
}

int btk_delete_page(struct btkey_t* btkey, int rpn)
{
    ushort rid = KEY_FREE_PAGEID;
    char buf[sizeof(ushort)+sizeof(int)];
    long ptr;

    /* キャッシュにあれば削除します。*/
    btk_cache_delete(btkey->page_cache, rpn);

    ptr = KEY_PAGE_OFFSET(rpn, btkey->pagesize);

    /* ファイルの最後を削除する場合はファイルサイズを小さくします。*/
    if (btkey->mmap->real_size == ptr+btkey->pagesize) {
        /* ファイルサイズを調整します。*/
        btkey->mmap->real_size = ptr;
        return 0;
    }

    /* 削除するページを空きページに更新します。*/
    memset(buf, '\0', sizeof(buf));
    memcpy(buf, &rid, sizeof(rid));
    if (btkey->free != 0)
        memcpy(&buf[sizeof(ushort)], &btkey->free, sizeof(int));

    mmap_seek(btkey->mmap, ptr);
    if (mmap_write(btkey->mmap, buf, sizeof(buf)) != sizeof(buf)) {
        err_write("btk_delete: write error.");
        return -1;
    }
    return btk_put_free(btkey, rpn);
}
