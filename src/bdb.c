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
#include "bdb.h"

/* B+木を用いたデータベースの関数群です。
 * 関数はスレッドセーフです。
 *
 * キーの長さは 1024 バイト以下に制限されています。
 * 重複キーは許可／不許可に設定できます。
 * 重複キーのデータはリンク形式でデータ部で管理されます。
 * キー順アクセスをサポートしています。
 *
 * キーはバイナリで比較されます。
 * キーの比較を行う関数を外部から設定できます。
 * キー、データともに可変長で扱われます。
 * データ長の最大は31ビット(2GB)まで格納可能です。
 *
 * リーフブロックにキーとデータをパックして格納することも可能です。
 * この場合のデータサイズは 255バイトに制限されています。
 * また、データパックの場合は重複索引は許可させません。
 *
 * エラーが発生した場合はエラーログに出力されます。
 *
 * 参考文献：bit別冊「ファイル構造」(1997)共立出版
 *
 *-------------------------------------------------------------------
 * B+木ノードの索引は右側のリーフに記録される。
 *
 * (root)                       +-+
 *                              |M|
 *                              +-+
 * (node)
 *             +-+-+                        +-+-+-+
 *             |E|I|                        |Q|U|Y|
 *             +-+-+                        +-+-+-+
 *
 * (leaf)
 * +-+-+-+-+ +-+-+-+-+ +-+-+-+-+  +-+-+-+-+ +-+-+-+-+ +-+-+-+-+ +-+-+
 * |A|B|C|D| |E|F|G|H| |I|J|K|L|  |M|N|O|P| |Q|R|S|T| |U|V|W|X| |Y|Z|
 * +-+-+-+-+ +-+-+-+-+ +-+-+-+-+  +-+-+-+-+ +-+-+-+-+ +-+-+-+-+ +-+-+
 *
 *-------------------------------------------------------------------
 * ブランチノード
 * +------+------+--------+----+---+-----+---+---+-----+---+---+
 * |0xBBEE|keynum|nodesize|(10)|ptr|ksize|key|ptr|ksize|key|ptr|...
 * +------+------+--------+----+---+-----+---+---+-----+---+---+
 *
 * リーフ
 * +------+------+--------+----+----+----+---+-----+---+----+-----+---+----+
 * |0xAAEE|keynum|nodesize|next|prev|flag|(9)|ksize|key|dptr|ksize|key|dptr|...
 * +------+------+--------+----+----+----+---+-----+---+----+-----+---+----+
 *
 * 2013/09/08
 * リーフノードのキー値にプレフィックス圧縮をサポートする。
 * 前のキー値と前方一致する値は保持しない方式としてキーサイズとプレフィックスサイズを保持する。
 * キーの復元方法は、pfksize分をデータを前のキーの先頭からコピーすることで復元する。
 * リーフの先頭キーは圧縮対象とはならない(pfksize == 0)。
 * リーフのflagにPREFIX_COMPRESS_NODEのビットが立っている場合は圧縮リーフとして処理する。
 * 新規に作成されるリーフはプロパティ値(NIO_PREFIX_COMPRESS)によって
 * プレフィックス圧縮されたノードとなる。
 *
 * 圧縮リーフ
 * +------+------+--------+----+----+----+---+-----+-------+---+----+
 * |0xAAEE|keynum|nodesize|next|prev|flag|(9)|ksize|pfksize|key|dptr|...
 * +------+------+--------+----+----+----+---+-----+-------+---+----+
 *  nodesize(2): 圧縮後のノードサイズ
 *  ksize(2): 実際のキーサイズ
 *  pfksize(1): プレフィックスサイズ(最大サイズは255)
 *
 *  キー値：00000001,00000002,00000010 の場合の圧縮リーフ
 * |--------------0---------------|------------1----------|------------3-----------|
 * +-------+----+------------+----+-------+----+-----+----+-------+----+------+----+
 * |ksize:8|pf:0|key:00000001|dptr|ksize:8|pf:7|key:2|dptr|ksize:8|pf:6|key:10|dptr|...
 * +-------+----+------------+----+-------+----+-----+----+-------+----+------+----+
 *
 *
 * 2013/10/05
 * リーフノードにアクセスする場合はリーフキャッシュを経由して行う事とした。
 * カーソルを作成してアクセスする場合もリーフキャッシュを経由する。
 * struct leaf_cache_t
 *
 * 2013/11/14
 * キーの削除を行った場合にキーの入れ替えや更新で bdb->node_pgsize を
 * オーバーする可能性があるので挿入時にノードを分割するリミットを
 * (bdb->node_pgsize - 64) とした。
 *
 * 2013/11/15
 * BDB_FILE_VERSION を 10 から 11 に変更した。
 *-------------------------------------------------------------------
 */

/* ヘッダー・ブロック */
#define BDB_HEADER_SIZE             64
#define BDB_FILEID                  "NBTK"
#define BDB_FILE_VERSION            11
#define BDB_TYPE_BTREE              0x02
#define BDB_TYPE_BTREE_DUPKEY       0x10
#define BDB_TYPE_BTREE_DATAPACK     0x20

#define BDB_VERSION_OFFSET          4
#define BDB_FILETYPE_OFFSET         6
#define BDB_TIMESTAMP_OFFSET        8
#define BDB_FREEPAGE_OFFSET         16
#define BDB_PAGESIZE_OFFSET         24
#define BDB_ALIGNMENT_OFFSET        28
#define BDB_ROOTPTR_OFFSET          30
#define BDB_LEAFTOP_OFFSET          38
#define BDB_LEAFBOT_OFFSET          46
#define BDB_FILESIZE_OFFSET         54

/* ブランチノード */
#define BDB_NODE_SIZE               16
#define BDB_NODE_ID                 0xBBEE

#define BDB_NODE_KEYNUM_OFFSET      2
#define BDB_NODE_SIZE_OFFSET        4
// aux: 10bytes
#define BDB_NODE_KEY_OFFSET         BDB_NODE_SIZE

/* リーフノード */
#define BDB_LEAF_SIZE               32
#define BDB_LEAF_ID                 0xAAEE

#define BDB_LEAF_KEYNUM_OFFSET      2
#define BDB_LEAF_SIZE_OFFSET        4
#define BDB_LEAF_NEXT_OFFSET        6
#define BDB_LEAF_PREV_OFFSET        14
#define BDB_LEAF_FLAG_OFFSET        22
// aux: 9bytes
#define BDB_LEAF_KEY_OFFSET         BDB_LEAF_SIZE

/* データ部 */
#define BDB_VALUE_SIZE              32

#define BDB_VALUE_ASIZE_OFFSET      0
#define BDB_VALUE_DSIZE_OFFSET      4
#define BDB_VALUE_NEXT_OFFSET       8
#define BDB_VALUE_PREV_OFFSET       16

/* ノードページサイズ */
#define DEFAULT_PAGE_SIZE           4096

#define BDB_KEY_NOTFOUND            0
#define BDB_KEY_FOUND               1

static int leaf_cache_flush(struct bdb_t* bdb);

static void set_default(struct bdb_t* bdb)
{
    bdb->node_pgsize = DEFAULT_PAGE_SIZE;
    bdb->mmap_view_size = MMAP_AUTO_SIZE;
    bdb->align_bytes = 16;           /* キーデータのアラインメント */
    bdb->filling_rate = 10;          /* 空き領域の充填率 */
    bdb->dupkey_flag = 0;            /* 重複索引なし */
    bdb->datapack_flag = 1;          /* データパックモード */
    bdb->prefix_compress_flag = 1;   /* リーフノードのプレフィックス圧縮 */

    bdb->root_ptr = 0;
    bdb->leaf_top_ptr = 0;
    bdb->leaf_bot_ptr = 0;
}

/*
 * データベースオブジェクトを作成します。
 *
 * 戻り値
 *  データベースオブジェクトのポインタを返します。
 *  メモリ不足の場合は NULL を返します。
 */
struct bdb_t* bdb_initialize(struct nio_t* nio)
{
    struct bdb_t* bdb;

    bdb = (struct bdb_t*)calloc(1, sizeof(struct bdb_t));
    if (bdb == NULL) {
        err_write("bdb: no memory.");
        return NULL;
    }
    bdb->nio = nio;

    /* キー比較関数の設定 */
    bdb->cmp_func = nio_cmpkey;

    /* プロパティの設定 */
    set_default(bdb);

    bdb->node_buf = (char*)malloc(bdb->node_pgsize);
    if (bdb->node_buf == NULL) {
        err_write("bdb: node buf no memory.");
        free(bdb);
        return NULL;
    }
    bdb->leaf_buf = (char*)malloc(bdb->node_pgsize);
    if (bdb->leaf_buf == NULL) {
        err_write("bdb: leaf buf no memory.");
        free(bdb->node_buf);
        free(bdb);
        return NULL;
    }
    bdb->leaf_cache = (struct leaf_cache_t*)calloc(sizeof(struct leaf_cache_t), 1);
    if (bdb->leaf_cache == NULL) {
        err_write("bdb: leaf cache no memory.");
        free(bdb->leaf_buf);
        free(bdb->node_buf);
        free(bdb);
        return NULL;
    }

    /* クリティカルセクションの初期化 */
    CS_INIT(&bdb->critical_section);

    return bdb;
}

/*
 * データベースオブジェクトを解放します。
 * 確保されていた領域が解放されます。
 *
 * bdb: データベースオブジェクトのポインタ
 *
 * 戻り値
 *  なし
 */
void bdb_finalize(struct bdb_t* bdb)
{
    /* クリティカルセクションの削除 */
    CS_DELETE(&bdb->critical_section);

    if (bdb->leaf_cache) {
        if (bdb->leaf_cache->keydata)
            free(bdb->leaf_cache->keydata);
        free(bdb->leaf_cache);
    }
    free(bdb->node_buf);
    free(bdb->leaf_buf);
    free(bdb);
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
 * bdb: データベースオブジェクトのポインタ
 * func: 関数のポインタ
 *
 * 戻り値
 *  なし
 */
void bdb_cmpfunc(struct bdb_t* bdb, CMP_FUNCPTR func)
{
    bdb->cmp_func = func;
}

/*
 * データベースのプロパティを設定します。
 *
 * プロパティ種類：
 *     NIO_PAGESIZE         ノードページサイズ
 *     NIO_MAP_VIEWSIZE     マップサイズ
 *     NIO_ALIGN_BYTES      キーデータ境界サイズ
 *     NIO_FILLING_RATE     データ充填率
 *     NIO_DUPLICATE_KEY    キー重複を許可(1 or 0)
 *     NIO_DATAPACK         キーとデータをパックして格納(1 or 0)
 *                          キー重複を許可している場合はデータパック不可
 *     NIO_PREFIX_COMPRESS  プレフィックス圧縮フラグ(1 or 0)
 *
 * bdb: データベースオブジェクトのポインタ
 * kind: プロパティ種類
 * value: 値
 *
 * 戻り値
 *  設定した場合はゼロを返します。エラーの場合は -1 を返します。
 */
int bdb_property(struct bdb_t* bdb, int kind, int value)
{
    int result = 0;

    switch (kind) {
        case NIO_PAGESIZE:
            if (value < 1024) {
                err_write("bdb_property: pagesize is too small, more than 1024 bytes.");
                return -1;
            }
            if (value > bdb->node_pgsize) {
                char* nb;
                char* lb;

                nb = (char*)malloc(value);
                lb = (char*)malloc(value);
                if (nb == NULL || lb == NULL) {
                    result = -1;
                    if (nb)
                        free(nb);
                    if (lb)
                        free(lb);
                } else {
                    free(bdb->node_buf);
                    free(bdb->leaf_buf);
                    bdb->node_buf = nb;
                    bdb->leaf_buf = lb;
                }
            }
            if (result == 0)
                bdb->node_pgsize = value;
            break;
        case NIO_MAP_VIEWSIZE:
            bdb->mmap_view_size = value * 1024 * 1024;
            break;
        case NIO_ALIGN_BYTES:
            bdb->align_bytes = value;
            break;
        case NIO_FILLING_RATE:
            bdb->filling_rate = value;
            break;
        case NIO_DUPLICATE_KEY:
            bdb->dupkey_flag = value;
            if (bdb->dupkey_flag)
                bdb->datapack_flag = 0;
            break;
        case NIO_DATAPACK:
            bdb->datapack_flag = value;
            if (bdb->dupkey_flag)
                bdb->datapack_flag = 0;
            break;
        case NIO_PREFIX_COMPRESS:
            bdb->prefix_compress_flag = value;
            break;
        default:
            result = -1;
            break;
    }
    return result;
}

static ushort recid(struct bdb_t* bdb, int64 ptr)
{
    ushort rid;

    mmap_seek(bdb->nio->mmap, ptr);
    if (mmap_read(bdb->nio->mmap, &rid, sizeof(ushort)) != sizeof(ushort))
        return 0;
    return rid;
}

static int is_node(struct bdb_t* bdb, int64 ptr)
{
    ushort rid;

    rid = recid(bdb, ptr);
    return (rid == BDB_NODE_ID);
}

static int is_leaf(struct bdb_t* bdb, int64 ptr)
{
    ushort rid;

    rid = recid(bdb, ptr);
    return (rid == BDB_LEAF_ID);
}

static int is_free(struct bdb_t* bdb, int64 ptr)
{
    ushort rid;

    rid = recid(bdb, ptr);
    return (rid == NIO_FREEDATA_ID);
}

static int is_eof(struct bdb_t* bdb, int64 ptr)
{
    return (ptr >= bdb->nio->mmap->real_size);
}

static int put_root(struct bdb_t* bdb, int64 ptr)
{
    bdb->root_ptr = ptr;

    mmap_seek(bdb->nio->mmap, BDB_ROOTPTR_OFFSET);
    if (mmap_write(bdb->nio->mmap, &bdb->root_ptr, sizeof(int64)) != sizeof(int64))
        return -1;
    return 0;
}

static int put_leaf_top(struct bdb_t* bdb, int64 ptr)
{
    bdb->leaf_top_ptr = ptr;

    mmap_seek(bdb->nio->mmap, BDB_LEAFTOP_OFFSET);
    if (mmap_write(bdb->nio->mmap, &bdb->leaf_top_ptr, sizeof(int64)) != sizeof(int64))
        return -1;
    return 0;
}

static int put_leaf_bot(struct bdb_t* bdb, int64 ptr)
{
    bdb->leaf_bot_ptr = ptr;

    mmap_seek(bdb->nio->mmap, BDB_LEAFBOT_OFFSET);
    if (mmap_write(bdb->nio->mmap, &bdb->leaf_bot_ptr, sizeof(int64)) != sizeof(int64))
        return -1;
    return 0;
}

static void update_filesize(struct bdb_t* bdb)
{
    bdb->filesize = nio_filesize(bdb->nio);
    mmap_seek(bdb->nio->mmap, BDB_FILESIZE_OFFSET);
    mmap_write(bdb->nio->mmap, &bdb->filesize, sizeof(bdb->filesize));
}

static void safe_check(struct bdb_t* bdb)
{
    int fileid_error = 0;

    if (bdb->root_ptr != 0) {
        if (! is_node(bdb, bdb->root_ptr))
            fileid_error = 1;
    }
    if (bdb->leaf_top_ptr != 0) {
        if (! is_leaf(bdb, bdb->leaf_top_ptr))
            fileid_error = 1;
    }
    if (bdb->leaf_bot_ptr != 0) {
        if (! is_leaf(bdb, bdb->leaf_bot_ptr))
            fileid_error = 1;
    }

    if (fileid_error) {
        err_write("bdb_open(safe_check): file id error");
        put_root(bdb, 0);
        put_leaf_top(bdb, 0);
        put_leaf_bot(bdb, 0);
    }
}

/*
 * データベースファイルをオープンします。
 *
 * bdb: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  オープンできた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int bdb_open(struct bdb_t* bdb, const char* fname)
{
    int fd;
    char buf[BDB_HEADER_SIZE];
    char fid[4];
    ushort fver;
    ushort ftype;
    int64 ctime;
    int64 freeptr;
    int page_size;
    ushort align_bytes;
    int64 rootptr;
    int64 leaftop;
    int64 leafbot;
    int64 filesize;

    fd = FILE_OPEN(fname, O_RDWR|O_BINARY);
    if (fd < 0) {
        err_write("bdb_open: file can't open: %s.", fname);
        return -1;
    }
    bdb->fd = fd;

    /* ヘッダー部の読み込み */
    if (FILE_READ(fd, buf, BDB_HEADER_SIZE) != BDB_HEADER_SIZE) {
        err_write("bdb_open: can't read header.");
        FILE_CLOSE(fd);
        return -1;
    }
    /* ファイル識別コード */
    memcpy(fid, buf, sizeof(fid));
    if (memcmp(fid, BDB_FILEID, sizeof(fid)) != 0) {
        err_write("bdb_open: illegal file.");
        FILE_CLOSE(fd);
        return -1;
    }
    /* ファイルバージョン */
    memcpy(&fver, &buf[BDB_VERSION_OFFSET], sizeof(fver));
    bdb->fver = fver;
    /* ファイルタイプ */
    memcpy(&ftype, &buf[BDB_FILETYPE_OFFSET], sizeof(ftype));
    bdb->dupkey_flag = (ftype & BDB_TYPE_BTREE_DUPKEY)? 1 : 0;
    bdb->datapack_flag = (ftype & BDB_TYPE_BTREE_DATAPACK)? 1 : 0;
    /* 作成日時 */
    memcpy(&ctime, &buf[BDB_TIMESTAMP_OFFSET], sizeof(ctime));
    /* 空き管理ページポインタ（8バイト）*/
    memcpy(&freeptr, &buf[BDB_FREEPAGE_OFFSET], sizeof(freeptr));
    bdb->nio->free_ptr = freeptr;
    /* ページサイズ（4バイト） */
    memcpy(&page_size, &buf[BDB_PAGESIZE_OFFSET], sizeof(page_size));
    bdb->node_pgsize = page_size;
    /* データのアラインメント（2バイト） */
    memcpy(&align_bytes, &buf[BDB_ALIGNMENT_OFFSET], sizeof(align_bytes));
    bdb->align_bytes = align_bytes;
    /* ルートポインタ（8バイト） */
    memcpy(&rootptr, &buf[BDB_ROOTPTR_OFFSET], sizeof(rootptr));
    bdb->root_ptr = rootptr;
    /* リーフの先頭ポインタ（8バイト） */
    memcpy(&leaftop, &buf[BDB_LEAFTOP_OFFSET], sizeof(leaftop));
    bdb->leaf_top_ptr = leaftop;
    /* リーフの最終ポインタ（8バイト） */
    memcpy(&leafbot, &buf[BDB_LEAFBOT_OFFSET], sizeof(leafbot));
    bdb->leaf_bot_ptr = leafbot;
    /* ファイルサイズ（8バイト） */
    memcpy(&filesize, &buf[BDB_FILESIZE_OFFSET], sizeof(filesize));
    bdb->filesize = filesize;
    if (bdb->filesize != 0) {
        /* 2011/12/17 ファイルサイズの調整 */
        FILE_TRUNCATE(fd, bdb->filesize);
    }

    /* メモリマップドファイルのオープン */
    bdb->nio->mmap = mmap_open(fd, MMAP_READWRITE, bdb->mmap_view_size);
    if (bdb->nio->mmap == NULL) {
        err_write("bdb_open: can't open mmap.");
        FILE_CLOSE(fd);
        return -1;
    }

    /* ファイルの整合性をチェックします。*/
    safe_check(bdb);
    return 0;
}

/*
 * データベースファイルを新規に作成します。
 * ファイルがすでに存在する場合でも新規に作成されます。
 *
 * bdb: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  オープンできた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int bdb_create(struct bdb_t* bdb, const char* fname)
{
    int fd;
    char buf[BDB_HEADER_SIZE];
    ushort fver = BDB_FILE_VERSION;
    ushort ftype = BDB_TYPE_BTREE;
    int64 ctime;

    fd = FILE_OPEN(fname, O_RDWR|O_CREAT|O_BINARY, CREATE_MODE);
    if (fd < 0) {
        err_write("bdb_create: file can't open: %s.", fname);
        return -1;
    }
    FILE_TRUNCATE(fd, 0);

    /* バッファのクリア */
    memset(buf, '\0', BDB_HEADER_SIZE);

    /* ファイル識別コード */
    memcpy(buf, BDB_FILEID, 4);
    /* ファイルバージョン（2バイト）*/
    memcpy(&buf[BDB_VERSION_OFFSET], &fver, sizeof(fver));
    /* ファイルタイプ（2バイト）*/
    if (bdb->dupkey_flag) {
        ftype |= BDB_TYPE_BTREE_DUPKEY;
    } else {
        if (bdb->datapack_flag)
            ftype |= BDB_TYPE_BTREE_DATAPACK;
    }
    memcpy(&buf[BDB_FILETYPE_OFFSET], &ftype, sizeof(ftype));
    /* 作成日時（8バイト） */
    ctime = system_time();
    memcpy(&buf[BDB_TIMESTAMP_OFFSET], &ctime, sizeof(ctime));
    /* ページサイズ（4バイト） */
    memcpy(&buf[BDB_PAGESIZE_OFFSET], &bdb->node_pgsize, sizeof(bdb->node_pgsize));
    /* アラインメント（2バイト） */
    memcpy(&buf[BDB_ALIGNMENT_OFFSET], &bdb->align_bytes, sizeof(bdb->align_bytes));

    /* ヘッダー部の書き出し */
    if (FILE_WRITE(fd, buf, BDB_HEADER_SIZE) != BDB_HEADER_SIZE) {
        err_write("bdb_create: can't write header.");
        FILE_CLOSE(fd);
        return -1;
    }

    /* メモリマップドファイルのオープン */
    bdb->nio->mmap = mmap_open(fd, MMAP_READWRITE, bdb->mmap_view_size);
    if (bdb->nio->mmap == NULL) {
        err_write("bdb_create: can't open mmap.");
        FILE_TRUNCATE(fd, 0);
        FILE_CLOSE(fd);
        return -1;
    }
    bdb->fd = fd;

    bdb->root_ptr = 0;
    bdb->leaf_top_ptr = 0;
    bdb->leaf_bot_ptr = 0;

    /* 空きデータ管理ページを書き出します。*/
    if (nio_create_free_page(bdb->nio) < 0)
        return -1;
    return 0;
}

/*
 * データベースファイルをクローズします。
 *
 * bdb: データベースオブジェクトのポインタ
 *
 * 戻り値
 *  なし
 */
void bdb_close(struct bdb_t* bdb)
{
    if (bdb->leaf_cache->update)
        leaf_cache_flush(bdb);

    mmap_close(bdb->nio->mmap);
    FILE_CLOSE(bdb->fd);
}

/*
 * データベースが存在するか調べます。
 *
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  データベースがが存在する場合は 1 を返します。
 *  存在しない場合はゼロを返します。
 */
int bdb_file(const char* fname)
{
    struct stat fstat;

    /* ファイル情報の取得 */
    if (stat(fname, &fstat) < 0)
        return 0;

    /* ディレクトリか調べます。*/
    if (S_ISDIR(fstat.st_mode))
        return 0;
    /* ファイルが存在する */
    return 1;
}

static int write_value(struct bdb_t* bdb,
                       int64 offset,
                       struct bdb_value_t* v,
                       const void* value)
{
    char buf[BDB_VALUE_SIZE];

    /* valueヘッダーを編集します。*/
    mmap_seek(bdb->nio->mmap, offset);
    memset(buf, '\0', BDB_VALUE_SIZE);

    /* 領域サイズ */
    memcpy(&buf[BDB_VALUE_ASIZE_OFFSET], &v->areasize, sizeof(int));
    /* データサイズ */
    memcpy(&buf[BDB_VALUE_DSIZE_OFFSET], &v->valsize, sizeof(int));
    /* 次ポインタ */
    memcpy(&buf[BDB_VALUE_NEXT_OFFSET], &v->next_ptr, sizeof(int64));
    /* 前ポインタ */
    memcpy(&buf[BDB_VALUE_PREV_OFFSET], &v->prev_ptr, sizeof(int64));

    /* valueヘッダーを書き出します。*/
    if (mmap_write(bdb->nio->mmap, buf, BDB_VALUE_SIZE) != BDB_VALUE_SIZE)
        return -1;

    if (value && v->valsize > 0) {
        int rbytes;

        /* 値を書き出します。*/
        if (mmap_write(bdb->nio->mmap, value, v->valsize) != v->valsize)
            return -1;
        rbytes = v->areasize - (BDB_VALUE_SIZE + v->valsize);
        if (rbytes > 0) {
            void* abuf;

            /* アライメント領域を書き出します。*/
            abuf = alloca(rbytes);
            memset(abuf, '\0', rbytes);
            if (mmap_write(bdb->nio->mmap, abuf, rbytes) != rbytes)
                return -1;
        }
    }
    return 0;
}

static int read_value_header(struct bdb_t* bdb,
                             int64 offset,
                             struct bdb_value_t* v)
{
    char buf[BDB_VALUE_SIZE];

    /* valueヘッダーを読み込みます。*/
    mmap_seek(bdb->nio->mmap, offset);
    if (mmap_read(bdb->nio->mmap, buf, BDB_VALUE_SIZE) != BDB_VALUE_SIZE)
        return -1;

    /* 領域サイズ */
    memcpy(&v->areasize, &buf[BDB_VALUE_ASIZE_OFFSET], sizeof(int));
    /* データサイズ */
    memcpy(&v->valsize, &buf[BDB_VALUE_DSIZE_OFFSET], sizeof(int));
    /* 次ポインタ */
    memcpy(&v->next_ptr, &buf[BDB_VALUE_NEXT_OFFSET], sizeof(int64));
    /* 前ポインタ */
    memcpy(&v->prev_ptr, &buf[BDB_VALUE_PREV_OFFSET], sizeof(int64));
    return 0;
}

static int write_value_header(struct bdb_t* bdb,
                              int64 offset,
                              struct bdb_value_t* v)
{
    char buf[BDB_VALUE_SIZE];

    memset(buf, '\0', sizeof(buf));
    /* 領域サイズ */
    memcpy(&buf[BDB_VALUE_ASIZE_OFFSET], &v->areasize, sizeof(int));
    /* データサイズ */
    memcpy(&buf[BDB_VALUE_DSIZE_OFFSET], &v->valsize, sizeof(int));
    /* 次ポインタ */
    memcpy(&buf[BDB_VALUE_NEXT_OFFSET], &v->next_ptr, sizeof(int64));
    /* 前ポインタ */
    memcpy(&buf[BDB_VALUE_PREV_OFFSET], &v->prev_ptr, sizeof(int64));

    /* valueヘッダーを書き込みます。*/
    mmap_seek(bdb->nio->mmap, offset);
    if (mmap_write(bdb->nio->mmap, buf, BDB_VALUE_SIZE) != BDB_VALUE_SIZE)
        return -1;
    return 0;
}

static int64 add_value(struct bdb_t* bdb,
                       const void* val,
                       int valsize,
                       int64 prev_ptr,
                       int64 next_ptr)
{
    int rsize, areasize;
    int64 ptr;
    struct bdb_value_t v;

    /* value を書き出す領域を取得します。*/
    rsize = BDB_VALUE_SIZE + valsize;
    if (bdb->align_bytes > 0) {
        if (rsize % bdb->align_bytes)
            rsize = (rsize / bdb->align_bytes + 1) * bdb->align_bytes;
    }
    ptr = nio_avail_space(bdb->nio, rsize, &areasize, bdb->filling_rate);
    if (ptr < 0)
        return -1;

    /* value を編集します。*/
    memset(&v, '\0', sizeof(struct bdb_value_t));
    v.areasize = areasize;
    v.valsize = valsize;
    v.next_ptr = next_ptr;
    v.prev_ptr = prev_ptr;

    /* value を書き出します。*/
    if (write_value(bdb, ptr, &v, val) < 0) {
        err_write("add_value: can't write value header.");
        return -1;
    }
    return ptr;
}

static int read_node(struct bdb_t* bdb, int64 offset, void* buf)
{
    mmap_seek(bdb->nio->mmap, offset);
    if (mmap_read(bdb->nio->mmap, buf, bdb->node_pgsize) != bdb->node_pgsize)
        return -1;
    return 0;
}

static int write_node(struct bdb_t* bdb, int64 offset, const void* buf)
{
    mmap_seek(bdb->nio->mmap, offset);
    if (mmap_write(bdb->nio->mmap, buf, bdb->node_pgsize) != bdb->node_pgsize)
        return -1;
    return 0;
}

static int get_node_size(const char* buf)
{
    ushort nsize;

    memcpy(&nsize, buf+BDB_NODE_SIZE_OFFSET, sizeof(ushort));
    return nsize;
}

static int get_node_keynum(const char* buf)
{
    ushort keynum;

    memcpy(&keynum, buf+BDB_NODE_KEYNUM_OFFSET, sizeof(ushort));
    return keynum;
}

static void set_node_id(char* buf)
{
    ushort rid = BDB_NODE_ID;

    memcpy(buf, &rid, sizeof(ushort));
}

static void set_node_size(char* buf, int nodesize)
{
    ushort nsize;

    nsize = (ushort)nodesize;
    memcpy(buf+BDB_NODE_SIZE_OFFSET, &nsize, sizeof(ushort));
}

static void set_node_keynum(char* buf, int keynum)
{
    ushort knum;

    knum = (ushort)keynum;
    memcpy(buf+BDB_NODE_KEYNUM_OFFSET, &knum, sizeof(ushort));
}

static void set_leaf_id(char* buf)
{
    ushort rid = BDB_LEAF_ID;

    memcpy(buf, &rid, sizeof(ushort));
}

static void set_leaf_size(char* buf, int size)
{
    ushort nsize;

    nsize = (ushort)size;
    memcpy(buf+BDB_LEAF_SIZE_OFFSET, &nsize, sizeof(ushort));
}

static void set_leaf_keynum(char* buf, int keynum)
{
    ushort knum;

    knum = (ushort)keynum;
    memcpy(buf+BDB_LEAF_KEYNUM_OFFSET, &knum, sizeof(ushort));
}

static void set_leaf_nextptr(char* buf, int64 ptr)
{
    memcpy(buf+BDB_LEAF_NEXT_OFFSET, &ptr, sizeof(int64));
}

static void set_leaf_prevptr(char* buf, int64 ptr)
{
    memcpy(buf+BDB_LEAF_PREV_OFFSET, &ptr, sizeof(int64));
}

static void set_leaf_flag(char* buf, uchar flag)
{
    memcpy(buf+BDB_LEAF_FLAG_OFFSET, &flag, sizeof(uchar));
}

#if 0
static void debug_nodebuf_print(struct bdb_t* bdb, const char* buf)
{
    char* p;
    char keybuf[NIO_MAX_KEYSIZE+1];
    int keynum;
    int nsize;
    int64 ptr;
    ushort ksize;
    int i;
    
    keynum = get_node_keynum(buf);
    nsize = get_node_size(buf);
    printf("[NODEBUF keynum=%d size=%d|", keynum, nsize);
    p = (char*)buf + BDB_NODE_KEY_OFFSET;
    
    for (i = 0; i < keynum; i++) {
        memcpy(&ptr, p, sizeof(int64));
        printf("ptr=%lld|", ptr);
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        printf("ksize=%d|", ksize);
        p += sizeof(ushort);
        memcpy(keybuf, p, ksize);
        keybuf[ksize] = '\0';
        printf("key=%s|", keybuf);
        p += ksize;
    }
    memcpy(&ptr, p, sizeof(int64));
    printf("ptr=%lld]\n", ptr);
}

static void debug_node_print(struct bdb_t* bdb, int64 node_ptr)
{
    char* buf;
    char* p;
    char keybuf[NIO_MAX_KEYSIZE+1];
    int keynum;
    int nsize;
    int64 ptr;
    ushort ksize;
    int i;

    buf = (char*)alloca(bdb->node_pgsize);
    if (read_node(bdb, node_ptr, buf) < 0)
        return;

    keynum = get_node_keynum(buf);
    nsize = get_node_size(buf);
    printf("[node_ptr=%lld keynum=%d size=%d|", node_ptr, keynum, nsize);
    p = buf + BDB_NODE_KEY_OFFSET;

    for (i = 0; i < keynum; i++) {
        memcpy(&ptr, p, sizeof(int64));
        printf("ptr=%lld|", ptr);
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        printf("ksize=%d|", ksize);
        p += sizeof(ushort);
        memcpy(keybuf, p, ksize);
        keybuf[ksize] = '\0';
        printf("key=%s|", keybuf);
        p += ksize;
    }
    memcpy(&ptr, p, sizeof(int64));
    printf("ptr=%lld]\n", ptr);
}

static void debug_leaf_print(struct bdb_t* bdb, int64 leaf_ptr)
{
    char* buf;
    int64 ptr;

    buf = (char*)alloca(bdb->node_pgsize);

    ptr = leaf_ptr;
    while (ptr != 0) {
        char* p;
        ushort rid, keynum, size;
        int64 next_ptr, prev_ptr;

        if (read_node(bdb, ptr, buf) < 0)
            break;

        p = buf;
        memcpy(&rid, p, sizeof(ushort));
        p += sizeof(ushort);
        memcpy(&keynum, p, sizeof(ushort));
        p += sizeof(ushort);
        memcpy(&size, p, sizeof(ushort));
        p += sizeof(ushort);
        memcpy(&next_ptr, p, sizeof(int64));
        p += sizeof(int64);
        memcpy(&prev_ptr, p, sizeof(int64));
        p += sizeof(int64);

        printf("LEAF [%lld] id=%x keynum=%d size=%d next=%lld prev=%lld\n",
            ptr, rid, keynum, size, next_ptr, prev_ptr);
        ptr = next_ptr;
    }
}

#endif

/*************
 * BTree I/O *
 *************/

static int bt_create_root(struct bdb_t* bdb,
                          const void* key,
                          int keysize,
                          int64 left_ptr,
                          int64 right_ptr)
{
    int64 ptr;
    char* buf;
    int knum = 1;
    int nsize;
    ushort ksz;
    char* p;

    /* ルートノードを書き出す領域を取得します。*/
    ptr = nio_avail_space(bdb->nio, bdb->node_pgsize, NULL, bdb->filling_rate);
    if (ptr < 0)
        return -1;

    /* ノードページを確保します。*/
    buf = (char*)alloca(bdb->node_pgsize);
    memset(buf, '\0', bdb->node_pgsize);

    /* ノードを編集します。*/
    set_node_id(buf);
    set_node_keynum(buf, knum);
    nsize = BDB_NODE_KEY_OFFSET + sizeof(int64) + sizeof(ushort) + keysize + sizeof(int64);
    set_node_size(buf, nsize);

    /* キー部を編集します。*/
    /* +--------+-------+---------+---------+
       |left_ptr|keysize|key-value|right_ptr|
       +--------+-------+---------+---------+
     */
    p = buf + BDB_NODE_KEY_OFFSET;
    memcpy(p, &left_ptr, sizeof(int64));
    p += sizeof(int64);
    ksz = (ushort)keysize;
    memcpy(p, &ksz, sizeof(ushort));
    p += sizeof(ushort);
    memcpy(p, key, keysize);
    p += keysize;
    memcpy(p, &right_ptr, sizeof(int64));

    /* ルートノードを書き出します。*/
    if (write_node(bdb, ptr, buf) < 0)
        return -1;

    /* ルートポインタを更新します。*/
    return put_root(bdb, ptr);
}

/* キーのオフセット位置を off_array に設定します。*/
static void bt_key_offset(const char* kbuf, int keynum, int* off_array)
{
    char* p;
    int offset = 0;

    p = (char*)kbuf;
    while (keynum--) {
        int64 left_ptr;
        ushort ksize;

        *off_array++ = offset;
        memcpy(&left_ptr, p, sizeof(int64));
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        p += sizeof(ushort) + ksize;
        offset += sizeof(int64) + sizeof(ushort) + ksize;
    }
}

static int bt_key_cmp(struct bdb_t* bdb,
                      const char* key,
                      int keysize,
                      const char* kbuf,
                      int offset,
                      int64* child_ptr)
{
    char* p;
    int64 left_ptr;
    ushort ksize;
    int c;

    p = (char*)kbuf + offset;
    memcpy(&left_ptr, p, sizeof(int64));
    p += sizeof(int64);
    memcpy(&ksize, p, sizeof(ushort));
    p += sizeof(ushort);
    c = (bdb->cmp_func)(key, keysize, p, ksize);
    if (c >= 0) {
        p += ksize;
        memcpy(child_ptr, p, sizeof(int64));
    } else if (c < 0) {
        *child_ptr = left_ptr;
    }
    return c;
}

/* ノードからキー値を検索します。
 * 等しいか大きい位置の child_ptr を返します。
 *
 * キーがマッチした場合で offset が NULL でない場合は
 * キーのオフセット位置を設定します。
 *
 * BDB_KEY_FOUND か BDB_KEY_NOTFOUND を返します。
 */
static int bt_search_node(struct bdb_t* bdb,
                          const char* buf,
                          const void* key,
                          int keysize,
                          int64* child_ptr,
                          int* offset)
{
    int keynum;
    char* p;
    int* off_array;
    int c;
    int start;
    int end;

    keynum = get_node_keynum(buf);
    p = (char*)buf + BDB_NODE_KEY_OFFSET;

    /* キーのオフセット位置を求めます。*/
    off_array = (int*)alloca(keynum * sizeof(int));
    bt_key_offset(p, keynum, off_array);

    /* 左端を調べます。*/
    c = bt_key_cmp(bdb, key, keysize, p, off_array[0], child_ptr);
    if (c == 0) {
        if (offset)
            *offset = off_array[0];
        return BDB_KEY_FOUND;   /* キーはノードにある */
    } else if (c < 0) {
        return BDB_KEY_NOTFOUND;    /* キーはノードにない */
    }

    /* 右端を調べます。*/
    c = bt_key_cmp(bdb, key, keysize, p, off_array[keynum-1], child_ptr);
    if (c == 0) {
        if (offset)
            *offset = off_array[keynum-1];
        return BDB_KEY_FOUND;   /* キーはノードにある */
    } else if (c > 0) {
        return BDB_KEY_NOTFOUND;    /* キーはノードにない */
    }

    /* ２分探索で探します。*/
    start = 1;
    end = keynum - 2;

    while (1) {
        int count;
        int mid;

        count = end - start + 1;
        if (count <= 2) {
            int i;

            for (i = start; i <= end; i++) {
                c = bt_key_cmp(bdb, key, keysize, p, off_array[i], child_ptr);
                if (c <= 0)
                    break;
            }
            if (c == 0) {
                if (offset)
                    *offset = off_array[i];
                return BDB_KEY_FOUND;
            }
            return BDB_KEY_NOTFOUND;
        }

        mid = start + count / 2;
        c = bt_key_cmp(bdb, key, keysize, p, off_array[mid], child_ptr);
        if (c < 0)
            end = mid;
        else if (c > 0)
            start = mid + 1;
        else {
            if (offset)
                *offset = off_array[mid];
            return BDB_KEY_FOUND;   /* キーはノードにあった */
        }
    }
}

static char* bt_set_key(char* buf,
                        const void* key,
                        int keysize,
                        int64 ptr)
{
    ushort ksize;

    ksize = (ushort)keysize;
    memcpy(buf, &ksize, sizeof(ushort));
    buf += sizeof(ushort);
    memcpy(buf, key, keysize);
    buf += keysize;
    memcpy(buf, &ptr, sizeof(int64));
    buf += sizeof(int64);
    return buf;
}

static void bt_ins_node(struct bdb_t* bdb,
                        char* buf,
                        const void* key,
                        int keysize,
                        int64 child_ptr)
{
    int ins_size;
    int keynum;
    char* p;
    int ins_done_flag = 0;
    int nkeynum;
    int nnsize;

    ins_size = sizeof(ushort) + keysize + sizeof(int64);
    keynum = get_node_keynum(buf);
    p = buf + BDB_NODE_KEY_OFFSET;
    while (keynum--) {
        int64 left_ptr;
        ushort ksize;
        int c;

        memcpy(&left_ptr, p, sizeof(int64));
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        p += sizeof(ushort);
        c = (bdb->cmp_func)(key, keysize, p, ksize);
        if (c <= 0) {
            size_t shift_n;

            p -= sizeof(ushort);  /* bugfix: 2011/4/2 */
            shift_n = (buf + get_node_size(buf)) - p;
            memmove(p+ins_size, p, shift_n);
            bt_set_key(p, key, keysize, child_ptr);
            ins_done_flag = 1;
            break;
        }
        p += ksize;
    }
    if (! ins_done_flag) {
        /* ノードの最後に追加します。*/
        p += sizeof(int64);
        bt_set_key(p, key, keysize, child_ptr);
    }

    /* ヘッダーを更新します。*/
    nkeynum = get_node_keynum(buf) + 1;
    nnsize = get_node_size(buf) + ins_size;
    set_node_keynum(buf, nkeynum);
    set_node_size(buf, nnsize);
}

static int bt_split_page(struct bdb_t* bdb,
                         const void* key,
                         int keysize,
                         int64 child_ptr,
                         char* buf,
                         void* promo_key,
                         int* promo_keysize,
                         int64* promo_child_ptr,
                         char* nbuf)
{
    char* wbuf;
    int wknum;
    int wnsize;
    char* midp;
    char* src;
    char* dst;
    ushort ksize;
    ushort knum;
    ushort nsize;

    /* 分割するノードを書き出す領域を取得します。*/
    *promo_child_ptr = nio_avail_space(bdb->nio, bdb->node_pgsize, NULL, bdb->filling_rate);
    if (*promo_child_ptr < 0)
        return -1;

    /* 作業領域の確保 */
    wbuf = (char*)alloca(bdb->node_pgsize * 2);

    /* ノードの内容を作業領域へ転記 */
    memcpy(wbuf, buf, bdb->node_pgsize);

    /* 作業領域にキーを挿入 */
    bt_ins_node(bdb, wbuf, key, keysize, child_ptr);
    wknum = get_node_keynum(wbuf);
    wnsize = get_node_size(wbuf);

    /* 昇進させる位置を決定 */
    midp = wbuf + bdb->node_pgsize / 2;

    /* 前半を元のノードへ編集 */
    src = wbuf + BDB_NODE_KEY_OFFSET;
    dst = buf + BDB_NODE_KEY_OFFSET;
    memset(dst, '\0', bdb->node_pgsize-BDB_NODE_KEY_OFFSET);

    knum = 0;
    nsize = 0;

    while (src < midp) {
        int n;

        memcpy(&ksize, src+sizeof(int64), sizeof(ushort));
        n = sizeof(int64) + sizeof(ushort) + ksize;
        memcpy(dst, src, n);
        src += n;
        dst += n;
        knum++;
        nsize += n;
    }
    /* 右側のポインタを移送 */
    memcpy(dst, src, sizeof(int64));
    src += sizeof(int64);
    dst += sizeof(int64);
    nsize += sizeof(int64);

    /* ヘッダー情報を更新 */
    set_node_size(buf, nsize+BDB_NODE_SIZE);
    set_node_keynum(buf, knum);

    /* 中心のキーを昇進させる */
    memcpy(&ksize, src, sizeof(ushort));
    src += sizeof(ushort);
    memcpy(promo_key, src, ksize);
    *promo_keysize = ksize;
    src += ksize;

    nsize += sizeof(ushort) + ksize;

    /* 後半を新しいノードへ転記 */
    memset(nbuf, '\0', bdb->node_pgsize);
    set_node_id(nbuf);

    dst = nbuf + BDB_NODE_SIZE;

    knum = wknum - (knum + 1);
    nsize = wnsize - (nsize + BDB_NODE_SIZE);

    memcpy(dst, src, nsize);

    /* ヘッダー情報を更新 */
    set_node_size(nbuf, nsize+BDB_NODE_SIZE);
    set_node_keynum(nbuf, knum);
    return 0;
}

static int bt_insert_key(struct bdb_t* bdb,
                         int64 search_ptr,
                         const void* key,
                         int keysize,
                         int64 node_ptr,
                         int64* promo_child_ptr,
                         void* promo_key,
                         int* promo_keysize)
{
    char* buf;
    int found;
    int promoted;
    int64 child_ptr;
    int64 p_b_ptr;
    void* p_b_key;
    int p_b_keysize;
    int rsize;

    if (is_leaf(bdb, search_ptr)) {
        memcpy(promo_key, key, keysize);
        *promo_keysize = keysize;
        *promo_child_ptr = node_ptr;
        return 1;
    }
    /* ノードページを確保します。*/
    buf = (char*)alloca(bdb->node_pgsize);

    if (read_node(bdb, search_ptr, buf) < 0)
        return -1;   /* error */

    found = bt_search_node(bdb, buf, key, keysize, &child_ptr, NULL);
    if (found == BDB_KEY_FOUND) {
        err_write("bt_insert_key: attempt to insert duplicate key.");
        return -1;
    }
    p_b_key = alloca(NIO_MAX_KEYSIZE);  /* 2011/12/12 keysize -> NIO_MAX_KEYSIZE */
    promoted = bt_insert_key(bdb,
                             child_ptr,
                             key,
                             keysize,
                             node_ptr,
                             &p_b_ptr,
                             p_b_key,
                             &p_b_keysize);
    // add error check 2013/11/12
    if (promoted < 0)
        return -1;

    if (! promoted)
        return 0;

    rsize = sizeof(ushort) + keysize + sizeof(int64);
    // 2013/11/14 削除時にキーの入れ替えが行われるので余裕を取っておく。
    if (get_node_size(buf) + rsize > (bdb->node_pgsize - 64)) {
        char* nbuf;

        /* ノードを２分割します。*/
        nbuf = (char*)alloca(bdb->node_pgsize);
        bt_split_page(bdb,
                      p_b_key,
                      p_b_keysize,
                      p_b_ptr,
                      buf,
                      promo_key,
                      promo_keysize,
                      promo_child_ptr,
                      nbuf);
        if (write_node(bdb, search_ptr, buf) < 0)
            return -1;
        if (write_node(bdb, *promo_child_ptr, nbuf) < 0)
            return -1;
        return 1;   /* 昇進 */
    }
    /* ノード内に挿入 */
    bt_ins_node(bdb, buf, p_b_key, p_b_keysize, p_b_ptr);
    if (write_node(bdb, search_ptr, buf) < 0)
        return -1;
    return 0;
}

static int bt_insert(struct bdb_t* bdb,
                     const void* key,
                     int keysize,
                     int64 node_ptr)
{
    int64 promo_child_ptr;
    void* promo_key;
    int promo_keysize;
    int promoted;

    promo_key = alloca(NIO_MAX_KEYSIZE);
    promoted = bt_insert_key(bdb,
                             bdb->root_ptr,
                             key,
                             keysize,
                             node_ptr,
                             &promo_child_ptr,
                             promo_key,
                             &promo_keysize);

    if (promoted) {
        /* 昇進したキーをルートに設定します。*/
        if (bt_create_root(bdb, promo_key, promo_keysize, bdb->root_ptr, promo_child_ptr) < 0)
            return -1;
    }
    return 0;
}

static int64 bt_search_key(struct bdb_t* bdb,
                           const char* key,
                           int keysize,
                           char* buf,
                           int* offset,
                           int64* child_ptr)
{
    int64 ptr;

    ptr = bdb->root_ptr;
    while (ptr > 0) {
        int found;

        if (read_node(bdb, ptr, buf) < 0)
            return -1;   /* error */

        found = bt_search_node(bdb,
                               buf,
                               key,
                               keysize,
                               child_ptr,
                               offset);
        if (found < 0)
            return -1;   /* error */
        if (found == BDB_KEY_FOUND) {
            /* found */
            return ptr;
        }
        if (is_leaf(bdb, *child_ptr))
            break;
        ptr = *child_ptr;
    }
    return 0;  /* notfound */
}

static void bt_delete_in_node(struct bdb_t* bdb,
                              char* buf,
                              int keyoff,
                              int lptr_del_flag)
{
    int keynum;
    int nsize;
    char* kbuf;
    char* p;
    int64 left_ptr;
    ushort ksize;
    int64 child_ptr;
    int dksize;
    int shift_s;

    keynum = get_node_keynum(buf);
    nsize = get_node_size(buf);

    kbuf = buf + BDB_NODE_SIZE;
    p = kbuf + keyoff;
    memcpy(&left_ptr, p, sizeof(int64));
    p += sizeof(int64);
    memcpy(&ksize, p, sizeof(ushort));
    p += sizeof(ushort) + ksize;
    memcpy(&child_ptr, p, sizeof(int64));

    dksize = sizeof(int64) + sizeof(ushort) + ksize;
    shift_s = nsize - BDB_NODE_SIZE - keyoff - dksize;
    if (shift_s > 0) {
        char* src;
        char* dst;

        /* shift */
        dst = kbuf + keyoff;
        if (! lptr_del_flag) {
            dst += sizeof(int64);
            shift_s -= sizeof(int64);
        }
        if (shift_s > 0) {
            src = dst + dksize;
            memmove(dst, src, shift_s);
        }
    }

    /* ノードのヘッダー情報を更新します。*/
    set_node_keynum(buf, keynum - 1);
    set_node_size(buf, nsize - dksize);
}

static char* bt_first_key(struct bdb_t* bdb,
                          char* buf,
                          ushort* ksize,
                          int64* lptr,
                          int64* rptr)
{
    char* p;
    char* keyp;

    p = buf + BDB_NODE_SIZE;
    memcpy(lptr, p, sizeof(int64));
    p += sizeof(int64);
    memcpy(ksize, p, sizeof(ushort));
    p += sizeof(ushort);
    keyp = p;
    p += *ksize;
    memcpy(rptr, p, sizeof(int64));
    return keyp;
}

/* 兄弟のポインタを返します。
   見つからない場合は -1 を返します。*/
static int64 bt_search_child(const char* node_buf,
                             int64 target_ptr,
                             int* keyoff,
                             int* right_node_flag)
{
    int keynum;
    char* p;
    int i;
    int64 ptr;
    int64 s_ptr = 0;

    /*
     +----+-------+-----+-----+----+
     |lptr|keysize| key | ... |rptr|
     +----+-------+-----+-----+----+
     */
    *right_node_flag = 0;
    keynum = get_node_keynum(node_buf);
    p = (char*)node_buf + BDB_NODE_SIZE;
    for (i = 0; i < keynum; i++) {
        ushort ksize;

        *keyoff = (int)(p - node_buf - BDB_NODE_SIZE);
        memcpy(&ptr, p, sizeof(int64));     /* left_ptr */
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        p += sizeof(ushort) + ksize;
        if (ptr == target_ptr) {
            memcpy(&s_ptr, p, sizeof(int64));
            return s_ptr;
        }
        s_ptr = ptr;
    }

    /* 右端 */
    memcpy(&ptr, p, sizeof(int64));
    if (ptr == target_ptr) {
        *right_node_flag = 1;
        return s_ptr;
    }
    return -1;
}

/* 親ノードのポインタを返します。*/
static int64 bt_search_parent_node(struct bdb_t* bdb,
                                   const void* key,
                                   int keysize,
                                   int64 target_ptr,
                                   int* p_keyoff,
                                   int64* s_ptr,
                                   int* right_node_flag)
{
    int64 p_ptr = 0;
    int64 ptr;
    char* buf;

    buf = (char*)alloca(bdb->node_pgsize);
    *right_node_flag = 0;

    ptr = bdb->root_ptr;
    while (ptr > 0) {
        int64 child_ptr;

        if (read_node(bdb, ptr, buf) < 0)
            return -1;  /* error */

        p_ptr = ptr;

        /* 子孫ポインタと一致するか？ */
        *s_ptr = bt_search_child(buf, target_ptr, p_keyoff, right_node_flag);
        if (*s_ptr > 0) {
            /* 親が見つかった。*/
            break;
        }

        /* 次に調べる子孫ノードを決めます。*/
        bt_search_node(bdb, buf, key, keysize, &child_ptr, NULL);
        ptr = child_ptr;
    }
    return p_ptr;
}

/* ノードの内容を入れ替えます。*/
static void bt_swap_node(struct bdb_t* bdb,
                         int64* ptr1,
                         char* buf1,
                         int64* ptr2,
                         char* buf2)
{
    char* tbuf;
    int64 t_ptr;

    tbuf = (char*)alloca(bdb->node_pgsize);

    /* tbuf <- buf1 */
    memcpy(tbuf, buf1, bdb->node_pgsize);
    t_ptr = *ptr1;

    /* buf1 <- buf2 */
    memcpy(buf1, buf2, bdb->node_pgsize);
    *ptr1 = *ptr2;

    /* buf2 <- tbuf */
    memcpy(buf2, tbuf, bdb->node_pgsize);
    *ptr2 = t_ptr;
}

/* 過疎状態になっているノードを連結して１ノードに収めます。
 *
 *   p_keyoff
 *   +
 *   +----+-------+-----+-----+----+
 *   |lptr|keysize| key | ... |rptr|
 *   +----+-------+-----+-----+----+
 *
 *   |<-(node)-------------->|<-(parent)-->|<-(sibling)----------->|
 *   +----+-------+-----+----+-------+-----+----+-------+-----+----+
 *   |lptr|keysize| key |rptr|keysize| key |lptr|keysize| key |rptr|
 *   +----+-------+-----+----+-------+-----+----+-------+-----+----+
 */
static void bt_cat_node(struct bdb_t* bdb,
                        char* node_buf,
                        char* p_buf,
                        int p_keyoff,
                        const char* s_buf)
{
    int keynum, p_keynum, s_keynum;
    int nsize, s_nsize, p_nsize;
    char* p;
    char* pp;
    char* sp;
    ushort p_ksize;
    char* p_key;

    keynum = get_node_keynum(node_buf);
    p_keynum = get_node_keynum(p_buf);
    s_keynum = get_node_keynum(s_buf);

    nsize = get_node_size(node_buf);
    p_nsize = get_node_size(p_buf);
    s_nsize = get_node_size(s_buf);

    pp = p_buf + BDB_NODE_SIZE + p_keyoff + sizeof(int64);
    memcpy(&p_ksize, pp, sizeof(ushort));
    p_key = pp + sizeof(ushort);

    /* 親のキーを node_buf の最後に追加します。
       子孫ポインタはコピーしません。*/
    p = node_buf + nsize;
    memcpy(p, pp, sizeof(ushort) + p_ksize);
    keynum++;
    nsize += sizeof(ushort) + p_ksize;
    p += sizeof(ushort) + p_ksize;

    /* ノードに追加した親のキーを削除します。*/
    bt_delete_in_node(bdb, p_buf, p_keyoff, 0);

    /* 兄弟のノードを追加します。*/
    sp = (char*)s_buf + BDB_NODE_SIZE;
    memcpy(p, sp, s_nsize - BDB_NODE_SIZE);
    keynum += s_keynum;
    nsize += (s_nsize - BDB_NODE_SIZE);

    /* ノードのキー数とサイズを更新します。*/
    set_node_keynum(node_buf, keynum);
    set_node_size(node_buf, nsize);
}

/* ノードバッファのキー領域を extsize 分シフトします。
   extsize がマイナスの場合は小さくなります。*/
static int bt_expand_keybuf(struct bdb_t* bdb,
                            char* buf,
                            int keyoff,
                            int keysize,
                            int extsize)
{
    int nsize;
    int m;
    char* src;
    char* dst;
    int shift_n;

    nsize = get_node_size(buf);

/* 2013/11/14 十分なサイズが確保されている。
    if (nsize+extsize > bdb->node_pgsize) {
        err_write("bt_expand_keybuf: key buffer size over.");
        return -1;
    }
*/
    m = BDB_NODE_SIZE + keyoff + sizeof(int64) + sizeof(ushort) + keysize;
    src = buf + m;
    dst = src + extsize;
    shift_n = nsize - m;
    memmove(dst, src, shift_n);
    set_node_size(buf, nsize+extsize);
    return 0;
}

static char* bt_center_key(const char* buf,
                           int bufsize,
                           int* lnum,
                           int* rnum)
{
    char* mp;
    char* p;
    char* midp;
    ushort ksize;
    char* endp;

    *lnum = 0;
    *rnum = 0;

    mp = (char*)buf + BDB_NODE_SIZE + (bufsize - BDB_NODE_SIZE) / 2;
    p = (char*)buf + BDB_NODE_SIZE;
    midp = p;
    while (p < mp) {
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        p += sizeof(ushort) + ksize;
        (*lnum)++;
        midp = p;
    }

    midp += sizeof(int64);
    memcpy(&ksize, midp, sizeof(ushort));
    endp = (char*)buf + bufsize - sizeof(int64);
    p = midp + sizeof(ushort) + ksize;
    while (p < endp) {
        p += sizeof(int64);
        memcpy(&ksize, p, sizeof(ushort));
        p += sizeof(ushort) + ksize;
        (*rnum)++;
    }
    return midp;
}

/* 過疎状態になっているページを再配分します。
 *
 *   w_buf                   midp                                  endp
 *   +----+-------+-----+----+-------+-----+----+-------+-----+----+
 *   |lptr|keysize| key |rptr|keysize| key |lptr|keysize| key |rptr|
 *   +----+-------+-----+----+-------+-----+----+-------+-----+----+
 *   |<----- left node ----->|<- parent -->|<------ right node --->|
 */
static void bt_redist_node(struct bdb_t* bdb,
                           char* node_buf,
                           char* p_buf,
                           int p_keyoff,
                           char* s_buf)
{
    char* w_buf;
    int w_keynum;
    int w_nsize;
    char* wp;
    char* pp;
    ushort p_ksize;
    ushort p_ksize2;
    int s_keynum;
    int s_nsize;
    int nsize;
    char* endp;
    char* midp;
    int lnum;
    int rnum;

    /* ワーク領域を確保する（3ノード分の大きさを確保） */
    w_buf = (char*)alloca(bdb->node_pgsize * 3);
    memset(w_buf, '\0', bdb->node_pgsize * 3);

    w_keynum = get_node_keynum(node_buf);

    /* ノードの内容をワーク領域にコピーします。*/
    nsize = get_node_size(node_buf);
    memcpy(w_buf, node_buf, nsize);
    wp = w_buf + nsize;
    w_nsize = nsize;

    /* 親のキーをワーク領域にコピーします。*/
    pp = p_buf + BDB_NODE_SIZE + p_keyoff;
    pp += sizeof(int64);    /* left ptr */
    memcpy(&p_ksize, pp, sizeof(ushort));
    memcpy(wp, pp, sizeof(ushort) + p_ksize);
    wp += sizeof(ushort) + p_ksize;
    w_nsize += sizeof(ushort) + p_ksize;
    w_keynum++;

    /* 兄弟の内容をすべてワーク領域にコピーします。*/
    s_keynum = get_node_keynum(s_buf);
    s_nsize = get_node_size(s_buf) - BDB_NODE_SIZE;
    memcpy(wp, s_buf+BDB_NODE_SIZE, s_nsize);
    wp += s_nsize;
    w_nsize += s_nsize;
    w_keynum += s_keynum;
    endp = wp;

    /* 中心のキーを親へ移します。*/
    midp = bt_center_key(w_buf, w_nsize, &lnum, &rnum);
    memcpy(&p_ksize2, midp, sizeof(ushort));
    /* 2011/12/01 キーサイズが変わったときの対処 */
    if (p_ksize2 != p_ksize) {
        int n;

        n = p_ksize2 - p_ksize;
        if (bt_expand_keybuf(bdb, p_buf, p_keyoff, p_ksize, n) < 0)
            return;
    }
    memcpy(pp, midp, sizeof(ushort) + p_ksize2);

    /* 前半を左ノードへ移します。*/
    memset(node_buf+BDB_NODE_SIZE, '\0', bdb->node_pgsize - BDB_NODE_SIZE);
    wp = w_buf + BDB_NODE_SIZE;
    nsize = (int)(midp - wp);
    memcpy(node_buf+BDB_NODE_SIZE, wp, nsize);
    set_node_size(node_buf, nsize+BDB_NODE_SIZE);
    set_node_keynum(node_buf, lnum);

    /* 後半を右ノードへ移します。*/
    midp += sizeof(ushort) + p_ksize2;
    memset(s_buf+BDB_NODE_SIZE, '\0', bdb->node_pgsize - BDB_NODE_SIZE);
    s_nsize = (int)(endp - midp);
    memcpy(s_buf+BDB_NODE_SIZE, midp, s_nsize);
    set_node_size(s_buf, s_nsize+BDB_NODE_SIZE);
    set_node_keynum(s_buf, rnum);
}

static int bt_adjust_node(struct bdb_t* bdb, int64 node_ptr, char* buf)
{
    char* keyp;
    ushort ksize;
    int64 lptr;
    int64 rptr;
    char* p_buf;
    char* s_buf;
    int64 p_ptr;
    int64 s_ptr;
    int p_keyoff;
    char* pp;
    ushort p_keysize;
    int right_node_flag;
    int nsize;

    nsize = get_node_size(buf);
    if (nsize > (bdb->node_pgsize / 2)) {
        /* 最小サイズより多いので調整の必要なし。*/
        return write_node(bdb, node_ptr, buf);
    }

    /* 過疎状態になっている。*/
    if (node_ptr == bdb->root_ptr) {
        int keynum;

        /* ルート */
        keynum = get_node_keynum(buf);
        if (keynum < 1) {
            /* キーが存在しなくなった。*/
            if (nio_add_free_list(bdb->nio, node_ptr, bdb->node_pgsize) < 0)
                return -1;
            return put_root(bdb, 0);
        }
        return write_node(bdb, node_ptr, buf);
    }

    /* 親を取得します。*/
    keyp = bt_first_key(bdb, buf, &ksize, &lptr, &rptr);
    p_ptr = bt_search_parent_node(bdb,
                                  keyp,
                                  ksize,
                                  node_ptr,
                                  &p_keyoff,
                                  &s_ptr,
                                  &right_node_flag);
    if (p_ptr < 0)
        return -1;
    if (p_ptr == 0)
        return write_node(bdb, node_ptr, buf);

    /* 親のページを読みます。*/
    p_buf = (char*)alloca(bdb->node_pgsize);
    if (read_node(bdb, p_ptr, p_buf) < 0)
        return -1;

    /* 兄弟(sibling)のページを取得します。*/
    s_buf = (char*)alloca(bdb->node_pgsize);
    if (read_node(bdb, s_ptr, s_buf) < 0)
        return -1;
    if (right_node_flag) {
        /* 対象が右端なのでノードの内容を入れ替えます。*/
        bt_swap_node(bdb, &node_ptr, buf, &s_ptr, s_buf);
    }

    /* 親のキーサイズを取得します。*/
    pp = p_buf + BDB_NODE_SIZE + p_keyoff + sizeof(int64);
    memcpy(&p_keysize, pp, sizeof(ushort));

    nsize = (get_node_size(buf) - BDB_NODE_SIZE) +
            (sizeof(ushort) + p_keysize) +
            (get_node_size(s_buf) - BDB_NODE_SIZE);
    if (nsize <= (bdb->node_pgsize - BDB_NODE_SIZE)) {
        /* 親のキーもノードに追加されるため
           ノードサイズが pgsize以下の場合にノードを連結します。*/
        bt_cat_node(bdb, buf, p_buf, p_keyoff, s_buf);
        if (write_node(bdb, node_ptr, buf) < 0)
            return -1;
        /* 兄弟(s_page)を削除します。*/
        if (nio_add_free_list(bdb->nio, s_ptr, bdb->node_pgsize) < 0)
            return -1;
        if (get_node_keynum(p_buf) < 1) {
            /* 親も削除します。*/
            if (nio_add_free_list(bdb->nio, p_ptr, bdb->node_pgsize) < 0)
                return -1;
            if (p_ptr == bdb->root_ptr) {
                /* ルートを更新します。*/
                if (put_root(bdb, node_ptr) < 0)
                    return -1;
                p_ptr = 0;
            }
        } else {
            if (write_node(bdb, p_ptr, p_buf) < 0)
                return -1;
        }
        if (p_ptr != 0) {
            /* 親ノードを再帰で処理します。*/
            if (bt_adjust_node(bdb, p_ptr, p_buf) < 0)
                return -1;
        }
        return 0;
    }

    /* 再配分します。*/
    bt_redist_node(bdb, buf, p_buf, p_keyoff, s_buf);

    /* 各ページを書き出します。*/
    if (write_node(bdb, p_ptr, p_buf) < 0)
        return -1;
    if (write_node(bdb, s_ptr, s_buf) < 0)
        return -1;
    if (write_node(bdb, node_ptr, buf) < 0)
        return -1;
    return 0;
}

/* B木の最下位ノード（リーフノードの上位）を取得します。*/
static int64 bt_get_leaf(struct bdb_t* bdb,
                         int64 ptr,
                         char* buf)
{
    int64 low_node_ptr;

    low_node_ptr = -1;
    while (ptr > 0) {
        if (read_node(bdb, ptr, buf) < 0)
            return -1;
        if (is_eof(bdb, ptr) || is_leaf(bdb, ptr) || is_free(bdb, ptr)) {
            if (read_node(bdb, low_node_ptr, buf) < 0)
                return -1;
            return low_node_ptr;
        }
        low_node_ptr = ptr;
        /* 左端のポインタを取得します。*/
        memcpy(&ptr, buf+BDB_NODE_SIZE, sizeof(int64));
    }
    return -1;
}

/* ノードバッファから keyoff 位置のキー値とキー長を取得します。*/
static void bt_get_key(const char* buf, int keyoff, char* key, ushort* ksize)
{
    char* p;

    p = (char*)buf + BDB_NODE_SIZE + keyoff + sizeof(int64);
    memcpy(ksize, p, sizeof(ushort));
    p += sizeof(ushort);
    memcpy(key, p, *ksize);
}

static void bt_put_key(char* buf, int keyoff, char* key, ushort ksize)
{
    char* p;

    p = buf + BDB_NODE_SIZE + keyoff + sizeof(int64);
    memcpy(p, &ksize, sizeof(ushort));
    p += sizeof(ushort);
    memcpy(p, key, ksize);
}

/* ノードとリーフのキーサイズとキーのみ入れ替えます。
   子孫ポインタは対象外です。*/
static int bt_swap_key(struct bdb_t* bdb,
                       char* nbuf1,
                       int keyoff1,
                       char* nbuf2,
                       int keyoff2)
{
    char* key1;
    ushort ksize1;
    char* key2;
    ushort ksize2;

    key1 = (char*)alloca(NIO_MAX_KEYSIZE);
    key2 = (char*)alloca(NIO_MAX_KEYSIZE);

    bt_get_key(nbuf1, keyoff1, key1, &ksize1);
    bt_get_key(nbuf2, keyoff2, key2, &ksize2);

    if (ksize1 != ksize2) {
        int n;

        /* shift key buffer */
        if (ksize1 < ksize2) {
            n = ksize2 - ksize1;
            if (bt_expand_keybuf(bdb, nbuf1, keyoff1, ksize1, n) < 0)
                return -1;
            if (bt_expand_keybuf(bdb, nbuf2, keyoff2, ksize2, -n) < 0)
                return -1;
        } else { /* (ksize1 > ksize2) */
            n = ksize1 - ksize2;
            if (bt_expand_keybuf(bdb, nbuf1, keyoff1, ksize1, -n) < 0)
                return -1;
            if (bt_expand_keybuf(bdb, nbuf2, keyoff2, ksize2, n) < 0)
                return -1;
        }
    }
    bt_put_key(nbuf1, keyoff1, key2, ksize2);
    bt_put_key(nbuf2, keyoff2, key1, ksize1);
    return 0;
}

static int bt_delete_key(struct bdb_t* bdb,
                         const char* key,
                         int keysize)
{
    int64 node_ptr;
    int keyoff;
    int64 child_ptr;
    int lptr_del_flag = 0;

    node_ptr = bt_search_key(bdb,
                             key,
                             keysize,
                             bdb->node_buf,
                             &keyoff,
                             &child_ptr);
    if (node_ptr <= 0)
        return -1; /* not found */

    /* 2011/12/02 先頭のキーを削除する場合は 1 とする。*/
    /* 2011/12/12 但し、ノードが削除された場合は 0 とする。*/
    if (keyoff == 0 && (! is_free(bdb, child_ptr)) && (! is_eof(bdb, child_ptr)))
        lptr_del_flag = 1;

    /* リーフノードが削除された場合もあるので空き領域かも判断します。*/
    if (is_eof(bdb, child_ptr)  ||
        is_leaf(bdb, child_ptr) || is_free(bdb, child_ptr)) {
        /* B木のリーフなのでそのまま削除 */
        bt_delete_in_node(bdb, bdb->node_buf, keyoff, lptr_del_flag);
        if (bt_adjust_node(bdb, node_ptr, bdb->node_buf) < 0)
            return -1;
    } else {
        char* work_buf;
        char* bt_leaf_buf;
        int64 bt_leaf_ptr;
        int wnsize;

        // B木のリーフと入れ換えてから削除
        // 2013/11/14
        // キー値を入れ替えた時にノードサイズをオーバーする可能性があるので倍のサイズを確保して処理する。
        work_buf = (char*)alloca(bdb->node_pgsize * 2);
        bt_leaf_buf = (char*)alloca(bdb->node_pgsize * 2);

        // B木のリーフを取得します。
        bt_leaf_ptr = bt_get_leaf(bdb, child_ptr, bt_leaf_buf);
        if (bt_leaf_ptr < 0)
            return -1;

        // キーを入れ替えます。
        memcpy(work_buf, bdb->node_buf, bdb->node_pgsize);
        if (bt_swap_key(bdb, work_buf, keyoff, bt_leaf_buf, 0) < 0)
            return -1;
        wnsize = get_node_size(work_buf);
        if (wnsize > bdb->node_pgsize) {
            err_write("bt_delete_key: node buffer size over! %d bytes.", wnsize);
            return -1;
        }
        memcpy(bdb->node_buf, work_buf, bdb->node_pgsize);
        if (write_node(bdb, node_ptr, bdb->node_buf) < 0)
            return -1;
        // 入れ替えたB木リーフのキーを削除します。
        lptr_del_flag = 1;  // 2011/12/12
        bt_delete_in_node(bdb, bt_leaf_buf, 0, lptr_del_flag);
        if (bt_adjust_node(bdb, bt_leaf_ptr, bt_leaf_buf) < 0)
            return -1;
    }
    return 0;
}

static int bt_update_key(struct bdb_t* bdb,
                         const void* key,
                         int keysize,
                         const void* new_key,
                         int new_keysize)
{
    int64 node_ptr;
    int keyoff;
    int64 child_ptr;

    node_ptr = bt_search_key(bdb,
                             key,
                             keysize,
                             bdb->node_buf,
                             &keyoff,
                             &child_ptr);
    if (node_ptr <= 0)
        return -1; /* not found */

    /* ノードからキーを削除 */
    bt_delete_in_node(bdb, bdb->node_buf, keyoff, 0);

    /* キーを挿入 */
    int inssize = sizeof(ushort) + new_keysize + sizeof(int64);
    if (get_node_size(bdb->node_buf) + inssize > bdb->node_pgsize) {
        // 2013/11/14 ノードがオーバーするため bt_insert() で挿入する。
        if (write_node(bdb, node_ptr, bdb->node_buf) < 0)
            return -1;
        if (bt_insert(bdb, new_key, new_keysize, child_ptr) < 0)
            return -1;
    } else {
        /* ノードに挿入 */
        bt_ins_node(bdb, bdb->node_buf, new_key, new_keysize, child_ptr);
        if (write_node(bdb, node_ptr, bdb->node_buf) < 0)
            return -1;
    }
    return 0;
}

static int bt_add_leaf_key(struct bdb_t* bdb,
                           const void* key,
                           int keysize,
                           struct bdb_leaf_t* leaf)
{
    if (bdb->root_ptr == 0) {
        /* ルートを作成します。*/
        return bt_create_root(bdb, key, keysize, leaf->prev_ptr, leaf->node_ptr);
    }

    /* B木にキーを追加します。*/
    return bt_insert(bdb, key, keysize, leaf->node_ptr);
}

/*****************
 * Leaf node I/O *
 *****************/

// 2013/09
/* 前方一致のサイズを返します。*/
static ushort prefix_keysize(int keysize,
                             const uchar* key,
                             int prevsize,
                             const uchar* prevkey)
{
    int i;

    for (i = 0; i < keysize && i < prevsize; i++) {
        if (key[i] != prevkey[i])
            break;
    }
    return (i > BDB_MAX_PREFIX_SIZE)? BDB_MAX_PREFIX_SIZE : (ushort)i;
}

/* キー圧縮したときのノードサイズを返します。*/
static int leaf_compress_size(struct bdb_t* bdb,
                              int keynum,
                              struct bdb_leaf_key_t* keydata,
                              int start)
{
    struct bdb_leaf_key_t* kp;
    struct bdb_leaf_key_t* kprev;
    int i;
    int size;
    
    size = 0;
    kp = &keydata[start];
    kprev = (start > 0)? &keydata[start-1] : NULL;

    for (i = start; i < keynum; i++) {
        // キー長
        size += kp->keysize;
        if (kprev == NULL) {
            // 圧縮なし
            size += sizeof(uchar);  // prefix size
            size += kp->keysize;
        } else {
            // prefix圧縮
            int n;
            
            size += sizeof(uchar);  // prefix size
            n = prefix_keysize(kp->keysize, kp->key, kprev->keysize, kprev->key);
            size += kp->keysize - n;
        }
        
        if (bdb->datapack_flag) {
            // packed data value
            size += sizeof(uchar);
            size += kp->value.u.pp.valsize;
        } else {
            // data ptr
            size += sizeof(int64);
        }
        kprev = kp;
        kp++;
    }
    return size;
}

// 2013/09
/* キー圧縮したシリアライズデータ(keybuf)を作成します。
   ノードサイズを返します。*/
static int leaf_compress_keydata(struct bdb_t* bdb,
                                 int keynum,
                                 struct bdb_leaf_key_t* keydata,
                                 char* keybuf)
{
    struct bdb_leaf_key_t* kp;
    struct bdb_leaf_key_t* kprev;
    char* p;
    int i;

    kp = keydata;
    p = keybuf;
    kprev = NULL;

    for (i = 0; i < keynum; i++) {
        ushort ksize;
        uchar pfksize;

        // キー長
        ksize = (ushort)kp->keysize;
        memcpy(p, &ksize, sizeof(ushort));
        p += sizeof(ushort);
        if (kprev == NULL) {
            // 圧縮なし
            pfksize = 0;
            memcpy(p, &pfksize, sizeof(uchar));
            p += sizeof(uchar);
            // キー
            memcpy(p, kp->key, kp->keysize);
            p += kp->keysize;
        } else {
            // prefix圧縮
            int n;
            ushort cksize;

            n = prefix_keysize(kp->keysize, kp->key, kprev->keysize, kprev->key);
            pfksize = (uchar)n;
            memcpy(p, &pfksize, sizeof(uchar));
            p += sizeof(uchar);
            // 圧縮キー
            cksize = ksize - n;
            memcpy(p, &kp->key[n], cksize);
            p += cksize;
        }

        if (bdb->datapack_flag) {
            // packed data value
            uchar dsize;

            dsize = (uchar)kp->value.u.pp.valsize;
            memcpy(p, &dsize, sizeof(uchar));
            p += sizeof(uchar);
            memcpy(p, kp->value.u.pp.val, dsize);
            p += dsize;
        } else {
            // data ptr
            memcpy(p, &kp->value.u.dp.v_ptr, sizeof(int64));
            p += sizeof(int64);
        }
        kprev = kp;
        kp++;
    }
    return (int)(p - keybuf);
}

/* キー圧縮なしのシリアライズデータのサイズを返します。*/
static int leaf_serialize_size(struct bdb_t* bdb,
                               int keynum,
                               struct bdb_leaf_key_t* keydata,
                               int start)
{
    struct bdb_leaf_key_t* kp;
    int i;
    int size;
    
    size = 0;
    kp = &keydata[start];

    for (i = start; i < keynum; i++) {
        // キー
        size += sizeof(ushort);
        size += kp->keysize;
        
        if (bdb->datapack_flag) {
            // packed data value
            size += sizeof(uchar);
            size += kp->value.u.pp.valsize;
        } else {
            // data ptr
            size += sizeof(int64);
        }
        kp++;
    }
    return size;
}

// 2013/09
/* キー圧縮なしのシリアライズデータ(keybuf)を作成します。
   ノードサイズを返します。*/
static int leaf_serialize_keydata(struct bdb_t* bdb,
                                  int keynum,
                                  struct bdb_leaf_key_t* keydata,
                                  char* keybuf)
{
    struct bdb_leaf_key_t* kp;
    char* p;
    int i;

    kp = keydata;
    p = keybuf;
    for (i = 0; i < keynum; i++) {
        ushort ksize;

        // キー長
        ksize = (ushort)kp->keysize;
        memcpy(p, &ksize, sizeof(ushort));
        p += sizeof(ushort);
        // キー
        memcpy(p, kp->key, kp->keysize);
        p += kp->keysize;

        if (bdb->datapack_flag) {
            // packed data value
            uchar dsize;

            dsize = (uchar)kp->value.u.pp.valsize;
            memcpy(p, &dsize, sizeof(uchar));
            p += sizeof(uchar);
            memcpy(p, kp->value.u.pp.val, dsize);
            p += dsize;
        } else {
            // data ptr
            memcpy(p, &kp->value.u.dp.v_ptr, sizeof(int64));
            p += sizeof(int64);
        }
        kp++;
    }
    return (int)(p - keybuf);
}

// 2013/09
/* キー圧縮されたシリアライズデータからkeydata(キーの配列)を作成します。*/
static void leaf_decompress_keybuf(struct bdb_t* bdb,
                                   int keynum,
                                   struct bdb_leaf_key_t* keydata,
                                   const char* keybuf)
{
    struct bdb_leaf_key_t* kp;
    struct bdb_leaf_key_t* kprev;
    char* p;
    int i;

    kp = keydata;
    p = (char*)keybuf;
    kprev = NULL;

    for (i = 0; i < keynum; i++) {
        ushort ksize;
        uchar pfksize;
        ushort cksize;

        memcpy(&ksize, p, sizeof(ushort));
        kp->keysize = ksize;
        p += sizeof(ushort);
        memcpy(&pfksize, p, sizeof(uchar));
        p += sizeof(uchar);

        cksize = ksize - pfksize;
        if (pfksize == 0) {
            // prefix圧縮なし
            memcpy(kp->key, p, ksize);
        } else {
            // 前のキーから省略した値を復元
            memcpy(kp->key, kprev->key, pfksize);
            memcpy(&kp->key[pfksize], p, cksize);
        }
        p += cksize;

        if (bdb->datapack_flag) {
            // packed data value
            uchar dsize;

            memcpy(&dsize, p, sizeof(uchar));
            kp->value.u.pp.valsize = dsize;
            p += sizeof(uchar);
            memcpy(kp->value.u.pp.val, p, dsize);
            p += dsize;
        } else {
            // data ptr
            memcpy(&kp->value.u.dp.v_ptr, p, sizeof(int64));
            p += sizeof(int64);
        }
        kprev = kp;
        kp++;
    }
}

// 2013/09
/* キー圧縮なしのシリアライズデータからkeydata(キーの配列)を作成します。*/
static void leaf_restore_keybuf(struct bdb_t* bdb,
                                int keynum,
                                struct bdb_leaf_key_t* keydata,
                                const char* keybuf)
{
    struct bdb_leaf_key_t* kp;
    char* p;
    int i;

    kp = keydata;
    p = (char*)keybuf;
    for (i = 0; i < keynum; i++) {
        ushort ksize;

        memcpy(&ksize, p, sizeof(ushort));
        kp->keysize = ksize;
        p += sizeof(ushort);
        memcpy(kp->key, p, kp->keysize);
        p += kp->keysize;
        if (bdb->datapack_flag) {
            // packed data value
            uchar dsize;

            memcpy(&dsize, p, sizeof(uchar));
            kp->value.u.pp.valsize = dsize;
            p += sizeof(uchar);
            memcpy(kp->value.u.pp.val, p, dsize);
            p += dsize;
        } else {
            // data ptr
            memcpy(&kp->value.u.dp.v_ptr, p, sizeof(int64));
            p += sizeof(int64);
        }
        kp++;
    }
}

// 2013/09
/* シリアライズデータ(keybuf)からkeydata(キー配列)を作成します。
   挿入用に leaf->keynum より大きいキー数を指定できるようにしてあります。
   keydataは動的にメモリ確保されるので使用後に解放する必要があります。
*/
static struct bdb_leaf_key_t* leaf_get_keydata(struct bdb_t* bdb,
                                               struct bdb_leaf_t* leaf,
                                               const char* keybuf,
                                               int keynum)
{
    struct bdb_leaf_key_t* keydata;

    if (keynum < leaf->keynum)
        return NULL;
    keydata = (struct bdb_leaf_key_t*)malloc(sizeof(struct bdb_leaf_key_t) * keynum);
    if (! keydata)
        return NULL;

    if (leaf->flag & PREFIX_COMPRESS_NODE)
        leaf_decompress_keybuf(bdb, leaf->keynum, keydata, keybuf);
    else
        leaf_restore_keybuf(bdb, leaf->keynum, keydata, keybuf);
    return keydata;
}

// 2013/09
/* keydata(key配列)をシリアライズしてkeybufに作成します。
   leafノードがprefix圧縮であればキーを圧縮してシリアライズします。*/
static int leaf_put_keydata(struct bdb_t* bdb,
                            struct bdb_leaf_t* leaf,
                            struct bdb_leaf_key_t* keydata,
                            char* keybuf)
{
    int size;

    if (leaf->flag & PREFIX_COMPRESS_NODE)
        size = leaf_compress_keydata(bdb, leaf->keynum, keydata, keybuf);
    else
        size = leaf_serialize_keydata(bdb, leaf->keynum, keydata, keybuf);
    return size;
}

// 2013/09
/* keydata(キー配列)をシリアライズした時のサイズを取得します。
   開始位置と任意のキー数を指定できるようになっています。
*/
static int leaf_sizeof_keybuf(struct bdb_t* bdb,
                              struct bdb_leaf_t* leaf,
                              int keynum,
                              struct bdb_leaf_key_t* keydata,
                              int start)
{
    int size;

    if (leaf->flag & PREFIX_COMPRESS_NODE)
        size = leaf_compress_size(bdb, keynum, keydata, start);
    else
        size = leaf_serialize_size(bdb, keynum, keydata, start);
    return size;
}

static int64 create_leaf(struct bdb_t* bdb,
                         struct bdb_leaf_t* prev,
                         int keynum,
                         struct bdb_leaf_key_t* keydata)
{
    int64 ptr;
    char* buf;
    int size;

    /* リーフノードを書き出す領域を取得します。*/
    ptr = nio_avail_space(bdb->nio, bdb->node_pgsize, NULL, bdb->filling_rate);
    if (ptr < 0)
        return -1;

    buf = (char*)alloca(bdb->node_pgsize);
    memset(buf, '\0', bdb->node_pgsize);

    set_leaf_id(buf);
    set_leaf_keynum(buf, keynum);
    if (bdb->prefix_compress_flag)
        set_leaf_flag(buf, PREFIX_COMPRESS_NODE);

    if (prev) {
        set_leaf_nextptr(buf, prev->next_ptr);
        set_leaf_prevptr(buf, prev->node_ptr);
    }

    /* キーをシリアライズします。*/
    if (bdb->prefix_compress_flag)
        size = leaf_compress_keydata(bdb, keynum, keydata, buf+BDB_LEAF_SIZE);
    else
        size = leaf_serialize_keydata(bdb, keynum, keydata, buf+BDB_LEAF_SIZE);

    set_leaf_size(buf, BDB_LEAF_SIZE+size);

    /* リーフノードを書き出します。*/
    if (write_node(bdb, ptr, buf) < 0)
        return -1;
    return ptr;
}

static int new_leaf(struct bdb_t* bdb,
                    const void* key,
                    int keysize,
                    const void* val,
                    int valsize)
{
    int64 vptr = 0;
    int64 ptr;
    struct bdb_leaf_key_t leafkey;

    if (! bdb->datapack_flag) {
        /* valueを書き出します。*/
        vptr = add_value(bdb, val, valsize, 0, 0);
        if (vptr < 0)
            return -1;
    }

    /* キーを編集します。*/
    leafkey.keysize = keysize;
    memcpy(leafkey.key, key, keysize);
    if (bdb->datapack_flag) {
        leafkey.value.u.pp.valsize = valsize;
        memcpy(leafkey.value.u.pp.val, val, valsize);
    } else {
        leafkey.value.u.dp.v_ptr = vptr;
    }

    ptr = create_leaf(bdb, NULL, 1, &leafkey);
    if (ptr < 0)
        return -1;

    /* リーフノードの先頭と最終ポインタを更新します。*/
    if (put_leaf_top(bdb, ptr) < 0)
        return -1;
    if (put_leaf_bot(bdb, ptr) < 0)
        return -1;
    return 0;
}

static int get_leaf(struct bdb_t* bdb,
                    int64 ptr,
                    struct bdb_leaf_t* leaf)
{
    char buf[BDB_LEAF_SIZE];
    ushort knum;
    ushort nsize;

    mmap_seek(bdb->nio->mmap, ptr);
    if (mmap_read(bdb->nio->mmap, buf, BDB_LEAF_SIZE) != BDB_LEAF_SIZE)
        return -1;

    leaf->node_ptr = ptr;
    memcpy(&knum, &buf[BDB_LEAF_KEYNUM_OFFSET], sizeof(ushort));
    leaf->keynum = knum;
    memcpy(&nsize, &buf[BDB_LEAF_SIZE_OFFSET], sizeof(ushort));
    leaf->nodesize = nsize;
    memcpy(&leaf->next_ptr, &buf[BDB_LEAF_NEXT_OFFSET], sizeof(int64));
    memcpy(&leaf->prev_ptr, &buf[BDB_LEAF_PREV_OFFSET], sizeof(int64));
    memcpy(&leaf->flag, &buf[BDB_LEAF_FLAG_OFFSET], sizeof(uchar));
    return 0;
}

static int update_leaf(struct bdb_t* bdb,
                       int64 ptr,
                       struct bdb_leaf_t* leaf)
{
    char buf[BDB_LEAF_SIZE];

    memset(buf, '\0', BDB_LEAF_SIZE);

    set_leaf_id(buf);
    set_leaf_keynum(buf, leaf->keynum);
    set_leaf_size(buf, leaf->nodesize);
    set_leaf_nextptr(buf, leaf->next_ptr);
    set_leaf_prevptr(buf, leaf->prev_ptr);
    set_leaf_flag(buf, leaf->flag);

    mmap_seek(bdb->nio->mmap, leaf->node_ptr);
    if (mmap_write(bdb->nio->mmap, buf, BDB_LEAF_SIZE) != BDB_LEAF_SIZE)
        return -1;
    return 0;
}

static int get_leaf_keybuf(struct bdb_t* bdb,
                           struct bdb_leaf_t* leaf,
                           char* keybuf)
{
    int64 ptr;
    int size;

    ptr = leaf->node_ptr + BDB_LEAF_SIZE;
    size = leaf->nodesize - BDB_LEAF_SIZE;
    mmap_seek(bdb->nio->mmap, ptr);
    if (mmap_read(bdb->nio->mmap, keybuf, size) != size)
        return -1;
    return 0;
}

static int put_leaf_keybuf(struct bdb_t* bdb,
                           struct bdb_leaf_t* leaf,
                           const char* keybuf)
{
    int64 ptr;
    int size;

    ptr = leaf->node_ptr + BDB_LEAF_SIZE;
    size = leaf->nodesize - BDB_LEAF_SIZE;
    mmap_seek(bdb->nio->mmap, ptr);
    if (mmap_write(bdb->nio->mmap, keybuf, size) != size)
        return -1;
    return 0;
}

static int leaf_key_cmp(struct bdb_t* bdb,
                        const char* key,
                        int keysize,
                        struct bdb_leaf_key_t* keydata)
{
    int c;

    c = (bdb->cmp_func)(key, keysize, keydata->key, keydata->keysize);
    return c;
}

static int leaf_cache_flush(struct bdb_t* bdb)
{
    struct leaf_cache_t* lc;
    int nodesize;
    
    lc = bdb->leaf_cache;
    if (! lc->update)
        return 0;

    nodesize = leaf_put_keydata(bdb, &lc->leaf, lc->keydata, bdb->leaf_buf);
    lc->leaf.nodesize = BDB_LEAF_SIZE + nodesize;
    
    if (put_leaf_keybuf(bdb, &lc->leaf, bdb->leaf_buf) < 0)
        return -1;
    if (update_leaf(bdb, lc->leaf.node_ptr, &lc->leaf) < 0)
        return -1;
    lc->update = 0;
    return 0;
}

static int leaf_cache_get_by_insert(struct bdb_t* bdb, int64 leaf_ptr)
{
    struct leaf_cache_t* lc;
    
    lc = bdb->leaf_cache;
    if (lc->leaf.node_ptr == leaf_ptr) {
        if (lc->leaf.keynum+1 > lc->alloc_keys) {
            int keynum = lc->leaf.keynum + 10;
            lc->keydata = (struct bdb_leaf_key_t*)realloc(lc->keydata, sizeof(struct bdb_leaf_key_t) * keynum);
            if (! lc->keydata)
                return -1;
            lc->alloc_keys = keynum;
        }
    } else {
        if (leaf_cache_flush(bdb) < 0)
            return -1;

        if (get_leaf(bdb, leaf_ptr, &lc->leaf) < 0)
            return -1;
        if (get_leaf_keybuf(bdb, &lc->leaf, bdb->leaf_buf) < 0)
            return -1;
        /* keydata(キー配列)を作成します。*/
        if (lc->keydata)
            free(lc->keydata);
        /* 挿入するため要素数を +1 する。*/
        lc->keydata = leaf_get_keydata(bdb, &lc->leaf, bdb->leaf_buf, lc->leaf.keynum+1);
        if (! lc->keydata)
            return -1;
        lc->alloc_keys = lc->leaf.keynum + 1;
    }
    return 0;
}

static int leaf_cache_get(struct bdb_t* bdb, int64 leaf_ptr)
{
    struct leaf_cache_t* lc;
    
    lc = bdb->leaf_cache;
    if (lc->leaf.node_ptr != leaf_ptr) {
        if (leaf_cache_flush(bdb) < 0)
            return -1;

        if (get_leaf(bdb, leaf_ptr, &lc->leaf) < 0)
            return -1;
        if (get_leaf_keybuf(bdb, &lc->leaf, bdb->leaf_buf) < 0)
            return -1;
        /* keydata(キー配列)を作成します。*/
        if (lc->keydata)
            free(lc->keydata);
        lc->alloc_keys = lc->leaf.keynum;
        lc->keydata = leaf_get_keydata(bdb, &lc->leaf, bdb->leaf_buf, lc->leaf.keynum);
        if (! lc->keydata)
            return -1;
    }
    return 0;
}

static void leaf_cache_clear(struct bdb_t* bdb)
{
    bdb->leaf_cache->leaf.node_ptr = 0;
    bdb->leaf_cache->update = 0;
}

static void make_slot(struct bdb_t* bdb,
                      int index,
                      int valsize,
                      const void* val,
                      int64 vptr,
                      struct bdb_slot_t* slot)
{
    slot->index = index;
    if (bdb->datapack_flag) {
        slot->u.pp.valsize = (uchar)valsize;
        if (val)
            memcpy(slot->u.pp.val, val, valsize);
        else
            memset(slot->u.pp.val, '\0', BDB_PACK_DATASIZE);
    } else {
        slot->u.dp.v_ptr = vptr;
    }
}

/* リーフノード内からキーを検索します。
 *
 * 見つかった場合はスロットの位置を設定して BDB_KEY_FOUND を返します。
 * 見つからなかった場合は挿入位置のスロットを設定して
 * BDB_KEY_NOTFOUND を返します。
 *
 * エラーの場合は -1 を返します。
 */
static int search_leaf(struct bdb_t* bdb,
                       struct bdb_leaf_t* leaf,
                       struct bdb_leaf_key_t* keydata,
                       const void* key,
                       int keysize,
                       struct bdb_slot_t* slot)
{
    struct bdb_leaf_key_t* kp;
    int c;
    int start;
    int end;
    int result = -1;

    if (leaf->keynum == 0) {
        make_slot(bdb, leaf->keynum, 0, NULL, 0, slot);
        result = BDB_KEY_NOTFOUND;
        goto final;
    }

    /* 左端を調べます。*/
    kp = &keydata[0];
    c = leaf_key_cmp(bdb, key, keysize, kp);
    if (c <= 0) {
        make_slot(bdb, 0, kp->value.u.pp.valsize, kp->value.u.pp.val,
                  kp->value.u.dp.v_ptr, slot);
        result = (c == 0)? BDB_KEY_FOUND : BDB_KEY_NOTFOUND;
        goto final;
    }

    /* 右端を調べます。*/
    kp = &keydata[leaf->keynum-1];
    c = leaf_key_cmp(bdb, key, keysize, kp);
    if (c == 0) {
        make_slot(bdb, leaf->keynum-1, kp->value.u.pp.valsize, kp->value.u.pp.val,
                  kp->value.u.dp.v_ptr, slot);
        result = BDB_KEY_FOUND;
        goto final;
    } else if (c > 0) {
        make_slot(bdb, leaf->keynum, 0, NULL, 0, slot);
        result = BDB_KEY_NOTFOUND;
        goto final;
    }

    /* ２分探索で探します。*/
    start = 1;
    end = leaf->keynum - 2;

    while (1) {
        int count;
        int mid;

        count = end - start + 1;
        if (count <= 2) {
            int i;

            for (i = start; i <= end; i++) {
                kp = &keydata[i];
                c = leaf_key_cmp(bdb, key, keysize, kp);
                if (c <= 0)
                    break;
            }
            make_slot(bdb, i, kp->value.u.pp.valsize, kp->value.u.pp.val,
                      kp->value.u.dp.v_ptr, slot);
            result = (c == 0)? BDB_KEY_FOUND : BDB_KEY_NOTFOUND;
            goto final;
        }

        mid = start + count / 2;
        kp = &keydata[mid];
        c = leaf_key_cmp(bdb, key, keysize, kp);
        if (c < 0)
            end = mid;
        else if (c > 0)
            start = mid + 1;
        else {
            /* 見つかった */
            make_slot(bdb, mid, kp->value.u.pp.valsize, kp->value.u.pp.val,
                      kp->value.u.dp.v_ptr, slot);
            result = BDB_KEY_FOUND;
            goto final;
        }
    }

final:
    return result;
}

static int insert_leaf_slot(struct bdb_t* bdb,
                            struct bdb_leaf_t* leaf,
                            struct bdb_leaf_key_t* keydata,
                            struct bdb_slot_t* slot,
                            struct bdb_leaf_key_t* inskey)
{
    if (slot->index < leaf->keynum) {
        int shift_n;

        shift_n = leaf->keynum - slot->index;
        memmove(&keydata[slot->index+1], &keydata[slot->index],
                sizeof(struct bdb_leaf_key_t) * shift_n);
    }

    /* insert */
    memcpy(&keydata[slot->index], inskey, sizeof(struct bdb_leaf_key_t));
    leaf->keynum++;
    return 0;
}

static int64 split_new_leaf(struct bdb_t* bdb,
                            struct bdb_leaf_t* baseleaf,
                            struct bdb_leaf_key_t* keydata,
                            int move_to)
{
    int nkeynum;
    struct bdb_leaf_key_t* nkeydata;
    int64 nptr;

    nkeynum = baseleaf->keynum - move_to;
    nkeydata = (struct bdb_leaf_key_t*)malloc(sizeof(struct bdb_leaf_key_t) * nkeynum);
    memcpy(nkeydata, &keydata[move_to], sizeof(struct bdb_leaf_key_t) * nkeynum);

    /* 新たなリーフを作成してチェーンにつなぎます。*/
    nptr = create_leaf(bdb, baseleaf, nkeynum, nkeydata);
    free(nkeydata);
    if (nptr < 0)
        return -1;
    return nptr;
}

static int split_leaf_index(struct bdb_t* bdb,
                            struct bdb_leaf_t* leaf,
                            struct bdb_leaf_key_t* keydata,
                            int split_size,
                            int* base_leafsize)
{
    int i;
    int start, mid;
    int kbufsize;
    
    /* 開始位置を調べます。*/
    mid = leaf->keynum / 2;
    kbufsize = leaf_sizeof_keybuf(bdb, leaf, mid, keydata, 0);
    start = (kbufsize > split_size)? 0 : mid;

    for (i = start; i < leaf->keynum; i++) {
        kbufsize = leaf_sizeof_keybuf(bdb, leaf, i+1, keydata, 0);
        if (kbufsize > split_size)
            return i;
        *base_leafsize = kbufsize;
    }
    return -1;
}

static int split_leaf(struct bdb_t* bdb,
                      struct bdb_leaf_t* leaf)
{
    int index;
    int split_size;
    struct bdb_leaf_key_t* keydata;
    int result = 0;
    int base_leafsize;

    split_size = (bdb->node_pgsize - BDB_LEAF_SIZE) / 3 * 2;  /* 2/3 */

    if (get_leaf_keybuf(bdb, leaf, bdb->leaf_buf) < 0)
        return -1;
    /* keydata(キー配列)を作成します。*/
    keydata = leaf_get_keydata(bdb, leaf, bdb->leaf_buf, leaf->keynum);
    if (! keydata)
        return -1;

    /* 分割する位置を求めます。*/
    index = split_leaf_index(bdb, leaf, keydata, split_size, &base_leafsize);
    if (index >= 0) {
        int64 nptr;
        int64 saved_next_ptr;

        /* 新たなリーフを作成して移動させます。*/
        nptr = split_new_leaf(bdb, leaf, keydata, index);
        if (nptr < 0) {
            result = -1;
            goto final;
        }
        saved_next_ptr = leaf->next_ptr;

        /* 元のリーフにつなげます。*/
        leaf->keynum = index;
        leaf->nodesize = BDB_LEAF_SIZE + base_leafsize;
        leaf->next_ptr = nptr;
        if (update_leaf(bdb, leaf->node_ptr, leaf) < 0) {
            result = -1;
            goto final;
        }
        /* 更新前の次リーフのprev_ptrを更新します。2011/12/10*/
        if (saved_next_ptr != 0) {
            struct bdb_leaf_t next_leaf;

            if (get_leaf(bdb, saved_next_ptr, &next_leaf) < 0) {
                result = -1;
                goto final;
            }
            next_leaf.prev_ptr = nptr;
            if (update_leaf(bdb, saved_next_ptr, &next_leaf) < 0) {
                result = -1;
                goto final;
            }
        }
    }

final:
    free(keydata);
    return result;
}

static void make_leaf_key(struct bdb_t* bdb,
                         const void* key,
                         int keysize,
                         int64 vptr,
                         const void* val,
                         int valsize,
                         struct bdb_leaf_key_t* kp)
{
    kp->keysize = keysize;
    memcpy(kp->key, key, keysize);

    if (bdb->datapack_flag) {
        kp->value.u.pp.valsize = (uchar)valsize;
        memcpy(kp->value.u.pp.val, val, valsize);
    } else {
        kp->value.u.dp.v_ptr = vptr;
    }
}

static int add_leaf_slot(struct bdb_t* bdb,
                         struct bdb_slot_t* slot,
                         const void* key,
                         int keysize,
                         const void* val,
                         int valsize)
{
    int rsize;
    int64 vptr = 0;
    int nodesize;
    struct bdb_leaf_key_t inskey;
    
    /* 挿入するサイズを取得します(圧縮は考慮しない)。
       キーサイズ(2) + プレフィックスサイズ(1) + キーサイズ
     */
    rsize = sizeof(ushort) + sizeof(uchar) + keysize;
    if (bdb->datapack_flag) {
        rsize += sizeof(uchar) + valsize;
    } else {
        /* value を書き出します。*/
        vptr = add_value(bdb, val, valsize, 0, 0);
        if (vptr < 0)
            return -1;
        rsize += sizeof(int64);
    }

    /* 挿入するキーを編集します。*/
    make_leaf_key(bdb, key, keysize, vptr, val, valsize, &inskey);

    /* 挿入後のリーフサイズを求めます。*/
    nodesize = BDB_LEAF_SIZE + leaf_sizeof_keybuf(bdb,
                                                  &bdb->leaf_cache->leaf,
                                                  bdb->leaf_cache->leaf.keynum,
                                                  bdb->leaf_cache->keydata,
                                                  0) + rsize;
    
    if (nodesize > bdb->node_pgsize) {
        struct bdb_leaf_t n_leaf;
        struct bdb_leaf_key_t* n_keydata;
        void* btkey;
        ushort btksize;

        if (leaf_cache_flush(bdb) < 0)
            return -1;

        /* リーフを分割します。*/
        if (split_leaf(bdb, &bdb->leaf_cache->leaf) < 0)
            return -1;

        /* 分割したリーフを取得 */
        if (get_leaf(bdb, bdb->leaf_cache->leaf.next_ptr, &n_leaf) < 0)
            return -1;
        
        if (get_leaf_keybuf(bdb, &n_leaf, bdb->leaf_buf) < 0)
            return -1;

        /* 分割したリーフのkeydata(キー配列)を作成します。*/
        n_keydata = leaf_get_keydata(bdb, &n_leaf, bdb->leaf_buf, n_leaf.keynum+1);
        if (! n_keydata)
            return -1;

        if (slot->index < bdb->leaf_cache->leaf.keynum) {
            /* 元のリーフに挿入 */
            if (insert_leaf_slot(bdb, &bdb->leaf_cache->leaf, bdb->leaf_cache->keydata, slot, &inskey) < 0)
                return -1;
        } else {
            struct bdb_slot_t n_slot;
            int n_nodesize;

            /* 分割したリーフの挿入位置を検索 */
            if (search_leaf(bdb, &n_leaf, n_keydata, key, keysize, &n_slot) < 0)
                return -1;
            if (insert_leaf_slot(bdb, &n_leaf, n_keydata, &n_slot, &inskey) < 0)
                return -1;
            /* 分割したリーフを書き出す */
            n_nodesize = leaf_put_keydata(bdb, &n_leaf, n_keydata, bdb->leaf_buf);
            n_leaf.nodesize = BDB_LEAF_SIZE + n_nodesize;
            if (put_leaf_keybuf(bdb, &n_leaf, bdb->leaf_buf) < 0)
                return -1;
            if (update_leaf(bdb, n_leaf.node_ptr, &n_leaf) < 0)
                return -1;
        }

        /* 分割したリーフの先頭キーをB木に追加 */
        btksize = (ushort)n_keydata[0].keysize;
        btkey = (void*)n_keydata[0].key;
        if (bt_add_leaf_key(bdb, btkey, btksize, &n_leaf) < 0) {
            free(n_keydata);
            return -1;
        }
        free(n_keydata);

        if (bdb->leaf_bot_ptr == bdb->leaf_cache->leaf.node_ptr) {
            /* 最終リーフのポインタを更新します。*/
            if (put_leaf_bot(bdb, bdb->leaf_cache->leaf.next_ptr) < 0)
                return -1;
        }
    } else {
        /* insert into leaf */
        if (insert_leaf_slot(bdb, &bdb->leaf_cache->leaf, bdb->leaf_cache->keydata, slot, &inskey) < 0)
            return -1;
    }
    return 0;
}

/* B木からキーを検索します。
 * 対象のリーフは bdb->leaf_cache に構築されます。
 *
 * BDB_KEY_NOTFOUND か BDB_KEY_FOUND を返します。
 * エラーの場合は -1 を返します。
 */
static int search_key(struct bdb_t* bdb,
                      const void* key,
                      int keysize,
                      struct bdb_slot_t* slot)
{
    int64 leaf_ptr = 0;

    if (bdb->root_ptr == 0) {
        /* B木は作成されておらず、リーフのみ存在の場合 */
        leaf_ptr = bdb->leaf_top_ptr;
    } else {
        int64 ptr;

        ptr = bdb->root_ptr;
        while (ptr > 0) {
            int64 child_ptr;

            if (read_node(bdb, ptr, bdb->node_buf) < 0)
                return -1;   /* error */

            bt_search_node(bdb, bdb->node_buf, key, keysize, &child_ptr, NULL);

            if (is_leaf(bdb, child_ptr)) {
                ptr = child_ptr;
                break;
            }
            ptr = child_ptr;
        }
        leaf_ptr = ptr;
    }
    if (leaf_ptr == 0)
        return BDB_KEY_NOTFOUND;

    if (leaf_cache_get(bdb, leaf_ptr) < 0)
        return -1;
    return search_leaf(bdb,
                       &bdb->leaf_cache->leaf,
                       bdb->leaf_cache->keydata,
                       key,
                       keysize,
                       slot);
}

/* 重複ありの場合のみ */
static int link_key_value(struct bdb_t* bdb,
                          const void* val,
                          int valsize,
                          struct bdb_slot_t* slot)
{
    int64 last_vptr;
    struct bdb_value_t v;
    int64 ptr;

    /* リンクリストの最後のデータを取得します。*/
    last_vptr = slot->u.dp.v_ptr;
    while (1) {
        if (read_value_header(bdb, last_vptr, &v) < 0)
            return -1;
        if (v.next_ptr == 0)
            break;
        last_vptr = v.next_ptr;
    }

    /* 新たな領域に書き出します。*/
    ptr = add_value(bdb, val, valsize, last_vptr, 0);
    if (ptr < 0)
        return -1;

    /* 最後のリストにつなぎます。*/
    v.next_ptr = ptr;
    if (write_value_header(bdb, last_vptr, &v) < 0)
        return -1;
    return 0;
}

/* データパック以外で使用 */
static int64 update_key_value(struct bdb_t* bdb,
                              const void* val,
                              int valsize,
                              struct bdb_slot_t* slot)
{
    int64 ptr;

    ptr = slot->u.dp.v_ptr;
    if (read_value_header(bdb, slot->u.dp.v_ptr, &slot->u.dp.v) < 0)
        return -1;

    if (valsize > slot->u.dp.v.areasize) {
        /* 元の領域に収まらないので別の領域に書き出します。*/

        /* 元の領域を開放します。*/
        if (nio_add_free_list(bdb->nio, slot->u.dp.v_ptr, slot->u.dp.v.areasize) < 0)
            return -1;
        /* 新たな領域に書き出します。*/
        ptr = add_value(bdb, val, valsize, slot->u.dp.v.prev_ptr, slot->u.dp.v.next_ptr);
        if (ptr < 0)
            return -1;
    } else {
        slot->u.dp.v.valsize = valsize;
        if (write_value(bdb, slot->u.dp.v_ptr, &slot->u.dp.v, val) < 0)
            return -1;
    }
    return ptr;
}

/* データパック以外で使用 */
static int delete_value(struct bdb_t* bdb, int64 ptr)
{
    while (ptr > 0) {
        struct bdb_value_t v;

        if (read_value_header(bdb, ptr, &v) < 0)
            return -1;
        if (nio_add_free_list(bdb->nio, ptr, v.areasize) < 0)
            return -1;
        ptr = v.next_ptr;
    }
    return 0;
}

static int delete_leaf(struct bdb_t* bdb, struct bdb_leaf_t* leaf)
{
    struct bdb_leaf_t s_leaf;

    /* リーフをつなぎ変えます。*/
    if (leaf->prev_ptr > 0) {
        if (get_leaf(bdb, leaf->prev_ptr, &s_leaf) < 0)
            return -1;
        s_leaf.next_ptr = leaf->next_ptr;
        if (update_leaf(bdb, s_leaf.node_ptr, &s_leaf) < 0)
            return -1;
    }
    if (leaf->next_ptr > 0) {
        if (get_leaf(bdb, leaf->next_ptr, &s_leaf) < 0)
            return -1;
        s_leaf.prev_ptr = leaf->prev_ptr;
        if (update_leaf(bdb, s_leaf.node_ptr, &s_leaf) < 0)
            return -1;
    }

    /* リーフ領域を開放します。*/
    if (nio_add_free_list(bdb->nio, leaf->node_ptr, bdb->node_pgsize) < 0)
        return -1;

    if (bdb->leaf_top_ptr == leaf->node_ptr) {
        if (put_leaf_top(bdb, leaf->next_ptr) < 0)
            return -1;
    }
    if (bdb->leaf_bot_ptr == leaf->node_ptr) {
        if (put_leaf_bot(bdb, leaf->prev_ptr) < 0)
            return -1;
    }
    return 0;
}

static int delete_leaf_slot(struct bdb_t* bdb,
                            struct bdb_slot_t* slot)
{
    if (slot->index >= bdb->leaf_cache->leaf.keynum)
        return -1;

    if (! bdb->datapack_flag) {
        /* データを削除します。*/
        if (delete_value(bdb, slot->u.dp.v_ptr) < 0)
            return -1;
    }

    if (slot->index < bdb->leaf_cache->leaf.keynum-1) {
        int shift_n;

        shift_n = bdb->leaf_cache->leaf.keynum - slot->index - 1;
        memmove(&bdb->leaf_cache->keydata[slot->index],
                &bdb->leaf_cache->keydata[slot->index+1],
                sizeof(struct bdb_leaf_key_t) * shift_n);
    }

    /* update leaf */
    bdb->leaf_cache->leaf.keynum--;

    if (bdb->leaf_cache->leaf.keynum == 0) {
        /* delete leaf */
        if (delete_leaf(bdb, &bdb->leaf_cache->leaf) < 0)
            return -1;
        leaf_cache_clear(bdb);
    } else {
        int nodesize;

        nodesize = leaf_put_keydata(bdb,
                                    &bdb->leaf_cache->leaf,
                                    bdb->leaf_cache->keydata,
                                    bdb->leaf_buf);
        bdb->leaf_cache->leaf.nodesize = BDB_LEAF_SIZE + nodesize;
        
        if (put_leaf_keybuf(bdb, &bdb->leaf_cache->leaf, bdb->leaf_buf) < 0)
            return -1;
        if (update_leaf(bdb, bdb->leaf_cache->leaf.node_ptr, &bdb->leaf_cache->leaf) < 0)
            return -1;
    }
    return 0;
}

static int update_key_value_pack(struct bdb_t* bdb,
                                 struct bdb_leaf_t* leaf,
                                 struct bdb_slot_t* slot,
                                 char* keybuf,
                                 struct bdb_leaf_key_t* keydata,
                                 const void* val,
                                 int valsize)
{
    struct bdb_leaf_key_t* kp;
    int nodesize;

    if (slot->index >= leaf->keynum)
        return -1;

    /* update value */
    kp = &keydata[slot->index];
    kp->value.u.pp.valsize = valsize;
    memcpy(kp->value.u.pp.val, val, valsize);

    /* update slot */
    slot->u.pp.valsize = valsize;
    memcpy(slot->u.pp.val, val, valsize);

    /* update leaf & keybuf */
    nodesize = leaf_put_keydata(bdb, leaf, keydata, keybuf);
    leaf->nodesize = BDB_LEAF_SIZE + nodesize;

    if (put_leaf_keybuf(bdb, leaf, keybuf) < 0)
        return -1;
    if (update_leaf(bdb, leaf->node_ptr, leaf) < 0)
        return -1;
    return 0;
}

static int get_first_leaf_key(struct bdb_t* bdb, int64 leaf_ptr, struct bdb_leaf_key_t* kp)
{
    if (leaf_cache_get(bdb, leaf_ptr) < 0)
        return -1;

    if (bdb->leaf_cache->leaf.keynum < 1)
        return -1;


    memcpy(kp, &bdb->leaf_cache->keydata[0], sizeof(struct bdb_leaf_key_t));
    return 0;
}

static int update_leaf_by_slot(struct bdb_t* bdb,
                               struct bdb_leaf_t* leaf,
                               struct bdb_slot_t* slot)
{
    char* kbuf;
    struct bdb_leaf_key_t* keydata;
    struct bdb_leaf_key_t* kp;
    int nodesize;
    
    if (slot->index >= leaf->keynum)
        return -1;

    kbuf = (char*)alloca(bdb->node_pgsize);
    if (get_leaf_keybuf(bdb, leaf, kbuf) < 0)
        return -1;

    /* keydata(キー配列)を作成します。*/
    keydata = leaf_get_keydata(bdb, leaf, kbuf, leaf->keynum);
    if (! keydata)
        return -1;

    kp = &keydata[slot->index];
    kp->value.u.dp.v_ptr = slot->u.dp.v_ptr;

    /* update leaf & keybuf */
    nodesize = leaf_put_keydata(bdb, leaf, keydata, kbuf);
    leaf->nodesize = BDB_LEAF_SIZE + nodesize;

    free(keydata);

    if (put_leaf_keybuf(bdb, leaf, kbuf) < 0)
        return -1;
    if (update_leaf(bdb, leaf->node_ptr, leaf) < 0)
        return -1;
    return 0;
}

/*
 * データベースからキーを検索します。
 * 重複キーが許可されている場合は最初のキーの値サイズを返します。
 *
 * bdb: データベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 *
 * キー値を存在していた場合は値のサイズを返します。
 * キーが存在しない場合は -1 を返します。
 */
int bdb_find(struct bdb_t* bdb, const void* key, int keysize)
{
    int dsize = -1;
    int result;
    struct bdb_slot_t slot;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("bdb_find: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&bdb->critical_section);

    result = search_key(bdb, key, keysize, &slot);
    if (result == BDB_KEY_FOUND) {
        if (bdb->datapack_flag) {
            dsize = slot.u.pp.valsize;
        } else {
            struct bdb_value_t v;

            if (read_value_header(bdb, slot.u.dp.v_ptr, &v) == 0)
                dsize = v.valsize;
        }
    }

    CS_END(&bdb->critical_section);
    return dsize;
}

/*
 * データベースからキーを検索して値をポインタに設定します。
 * 重複キーが許可されている場合は最初のキーの値を取得します。
 *
 * bdb: データベース構造体のポインタ
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
int bdb_get(struct bdb_t* bdb, const void* key, int keysize, void* val, int valsize)
{
    int dsize = -1;
    int status;
    struct bdb_slot_t slot;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("bdb_get: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&bdb->critical_section);

    status = search_key(bdb, key, keysize, &slot);
    if (status == BDB_KEY_FOUND) {
        if (bdb->datapack_flag) {
            if (valsize < slot.u.pp.valsize)
                dsize = -2;
            else {
                dsize = slot.u.pp.valsize;
                memcpy(val, slot.u.pp.val, dsize);
            }
        } else {
            struct bdb_value_t v;

            if (read_value_header(bdb, slot.u.dp.v_ptr, &v) == 0) {
                if (valsize < v.valsize)
                    dsize = -2;
                else {
                    if (mmap_read(bdb->nio->mmap, val, v.valsize) == v.valsize)
                        dsize = v.valsize;
                }
            }
        }
    }

    CS_END(&bdb->critical_section);
    return dsize;
}

/*
 * データベースからキーを検索して値のポインタを返します。
 * 値の領域は関数内で確保されます。
 * 値のポインタは使用後に bdb_free() で解放する必要があります。
 * 重複キーが許可されている場合は最初のキーの値を取得します。
 *
 * bdb: データベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * valsize: 値の領域サイズが設定されるポインタ
 *
 * キー値を取得できた場合は値のサイズを設定して領域のポインタを返します。
 * キーが存在しない場合は valsize に -1 が設定されて NULL を返します。
 * その他のエラーの場合は valsize に -2 が設定されて NULL を返します。
 */
void* bdb_aget(struct bdb_t* bdb, const void* key, int keysize, int* valsize)
{
    void* val = NULL;
    int status;
    struct bdb_slot_t slot;

    *valsize = -2;
    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("bdb_aget: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return NULL;
    }

    CS_START(&bdb->critical_section);

    status = search_key(bdb, key, keysize, &slot);
    if (status == BDB_KEY_FOUND) {
        if (bdb->datapack_flag) {
            val = malloc(slot.u.pp.valsize);
            if (val == NULL) {
                err_write("bdb_aget: no memory %d bytes.", slot.u.pp.valsize);
                goto final;
            }
            memcpy(val, slot.u.pp.val, slot.u.pp.valsize);
            *valsize = slot.u.pp.valsize;
        } else {
            struct bdb_value_t v;

            if (read_value_header(bdb, slot.u.dp.v_ptr, &v) == 0) {
                val = malloc(v.valsize);
                if (val == NULL) {
                    err_write("bdb_aget: no memory %d bytes.", v.valsize);
                    goto final;
                }
                if (mmap_read(bdb->nio->mmap, val, v.valsize) != v.valsize) {
                    err_write("bdb_aget: can't mmap_read.");
                    free(val);
                    val = NULL;
                    goto final;
                }
                *valsize = v.valsize;
            }
        }
    } else {
        /* not found */
        *valsize = -1;
    }

final:
    CS_END(&bdb->critical_section);
    return val;
}

/*
 * データベースにキーと値を設定します。
 *
 * 重複キーが許可されていない場合でキーがすでに存在している場合は
 * 値が置換されます。
 *
 * bdb: データベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値のサイズ
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int bdb_put(struct bdb_t* bdb, const void* key, int keysize, const void* val, int valsize)
{
    int result = 0;
    int status;
    struct bdb_slot_t slot;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("bdb_put: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }
    if (bdb->datapack_flag) {
        if (valsize > BDB_PACK_DATASIZE) {
            err_write("bdb_put: valsize is too large, less than %d bytes.", BDB_PACK_DATASIZE);
            return -1;
        }
    }

    CS_START(&bdb->critical_section);

    /* キーを検索します。*/
    status = search_key(bdb, key, keysize, &slot);

    // 2013/11/14 add check
    if (status < 0) {
        result = -1;
        goto final;
    }
    if (status == BDB_KEY_FOUND) {
        if (bdb->dupkey_flag) {
            /* データ部をリンクでつなぎます。*/
            result = link_key_value(bdb, val, valsize, &slot);
        } else {
            if (bdb->datapack_flag) {
                if (update_key_value_pack(bdb, &bdb->leaf_cache->leaf, &slot, bdb->leaf_buf,
                                          bdb->leaf_cache->keydata, val, valsize) < 0) {
                    err_write("bdb_put: update_key_value_pack() is fail.");
                    result = -1;
                    goto final;
                }
            } else {
                int64 ptr;

                /* 値を置換します。*/
                ptr = update_key_value(bdb, val, valsize, &slot);
                if (ptr < 0) {
                    result = -1;
                    goto final;
                }
                if (ptr != slot.u.dp.v_ptr) {
                    /* 領域が変わったのでリーフキーを更新する。 */
                    slot.u.dp.v_ptr = ptr;
                    if (update_leaf_by_slot(bdb, &bdb->leaf_cache->leaf, &slot) < 0) {
                        result = -1;
                        goto final;
                    }
                }
            }
        }
    } else {
        /* 新規挿入 */
        if (bdb->root_ptr == 0) {
            if (bdb->leaf_top_ptr == 0) {
                /* リーフを新規作成 */
                if (new_leaf(bdb, key, keysize, val, valsize) < 0) {
                    result = -1;
                    goto final;
                }
            } else {
                /* リーフを取得（リーフノードは１個しかない） */
                if (leaf_cache_get_by_insert(bdb, bdb->leaf_top_ptr) < 0) {
                    result = -1;
                    goto final;
                }
                /* 該当のスロットを作成 */
                if (search_leaf(bdb, &bdb->leaf_cache->leaf, bdb->leaf_cache->keydata,
                                key, keysize, &slot) < 0) {
                    result = -1;
                    goto final;
                }
                /* スロットが示すリーフへ追加 */
                if (add_leaf_slot(bdb, &slot, key, keysize, val, valsize) < 0) {
                    result = -1;
                    goto final;
                }
                bdb->leaf_cache->update = 1;
            }
        } else {
            if (leaf_cache_get_by_insert(bdb, bdb->leaf_cache->leaf.node_ptr) < 0) {
                result = -1;
                goto final;
            }
            /* スロットが示すリーフへ追加 */
            if (add_leaf_slot(bdb, &slot, key, keysize, val, valsize) < 0) {
                result = -1;
               goto final;
            }
            bdb->leaf_cache->update = 1;
        }
    }

final:
    update_filesize(bdb);
    CS_END(&bdb->critical_section);
    return result;
}

/*
 * データベースからキーを削除します。
 * 重複キーが許可されている場合はすべて削除されます。
 * 削除された領域は再利用されます。
 *
 * bdb: データベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int bdb_delete(struct bdb_t* bdb, const void* key, int keysize)
{
    int result = 0;
    int status;
    struct bdb_slot_t slot;
    int top_leaf_flag;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("bdb_delete: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&bdb->critical_section);
    
    /* キーを検索します。*/
    status = search_key(bdb, key, keysize, &slot);
    if (status < 0) {
        err_write("bdb_delete: search_key() is fail.");
        result = -1;
        goto final;
    }
    if (status != BDB_KEY_FOUND) {
        /* not found */
        result = -1;
        goto final;
    }

    /* 削除対象のキーが先頭リーフか調べておきます。*/
    top_leaf_flag = (bdb->leaf_top_ptr == bdb->leaf_cache->leaf.node_ptr);

    /* リーフからデータとキーを削除します。*/
    if (delete_leaf_slot(bdb, &slot) < 0) {
        err_write("bdb_delete: delete_leaf_slot() is fail.");
        result = -1;
        goto final;
    }

    /* リーフの状態からB木を更新します。*/
    /*------------------------------------
       type1 先頭リーフが削除された場合
             次リーフの先頭キーをB木から削除(left_ptr削除)
       type2 先頭以外のリーフが削除された場合
             削除されたリーフの最初のキーをB木から削除(right_ptr削除)
       type3 リーフは削除されないが先頭のキーが削除された場合
             削除されたキーをB木から削除(right_ptr削除)
             削除された次のキーをB木に挿入
       type4 それ以外はB木の更新は必要ない
    ------------------------------------
    */

    if (bdb->leaf_cache->leaf.keynum == 0) {
        /* リーフが削除された */
        if (top_leaf_flag) {
            /* type1 */
            if (bdb->leaf_cache->leaf.next_ptr > 0) {
                struct bdb_leaf_key_t kp;

                if (get_first_leaf_key(bdb, bdb->leaf_cache->leaf.next_ptr, &kp) < 0) {
                    result = -1;
                    goto final;
                }
                if (bt_delete_key(bdb, (const char*)kp.key, kp.keysize) < 0) {
                    result = -1;
                    goto final;
                }
            }
        } else {
            /* type2 */
            if (bt_delete_key(bdb, key, keysize) < 0) {
                result = -1;
                goto final;
            }
        }
    } else {
        if (slot.index == 0 && (! top_leaf_flag)) {
            /* type3 */
            struct bdb_leaf_key_t kp;

            if (get_first_leaf_key(bdb, bdb->leaf_cache->leaf.node_ptr, &kp) < 0) {
                result = -1;
                goto final;
            }
            if (bt_update_key(bdb, key, keysize, kp.key, kp.keysize) < 0) {
                result = -1;
                goto final;
            }
        }
    }

final:
    update_filesize(bdb);
    CS_END(&bdb->critical_section);
    return result;
}

/*
 * 関数内で確保された領域を開放します。
 */
void bdb_free(const void* v)
{
    if (v) {
        free((void*)v);
    }
}


static int cursor_get_slot(struct dbcursor_t* cur, int index)
{
    struct bdb_t* bdb;
    struct bdb_leaf_key_t* kp;

    bdb = cur->bdb;
    cur->index = index;
    cur->slot.index = cur->index;
    if (cur->index >= bdb->leaf_cache->leaf.keynum)
        return 0;

    kp = &bdb->leaf_cache->keydata[index];

    if (cur->bdb->datapack_flag) {
        cur->slot.u.pp.valsize = kp->value.u.pp.valsize;
        memcpy(cur->slot.u.pp.val, kp->value.u.pp.val, kp->value.u.pp.valsize);
    } else {
        cur->slot.u.dp.v_ptr = kp->value.u.dp.v_ptr;
        if (read_value_header(cur->bdb, cur->slot.u.dp.v_ptr, &cur->slot.u.dp.v) < 0)
            return -1;
    }
    return 0;
}

static int cursor_leaf_top(struct dbcursor_t* cur, int64 ptr)
{
    if (leaf_cache_get(cur->bdb, ptr) < 0)
        return -1;
    if (cursor_get_slot(cur, 0) < 0)
        return -1;
    cur->node_ptr = ptr;
    return 0;
}

static int cursor_leaf_bot(struct dbcursor_t* cur, int64 ptr)
{
    if (leaf_cache_get(cur->bdb, ptr) < 0)
        return -1;
    if (cursor_get_slot(cur, cur->bdb->leaf_cache->leaf.keynum-1) < 0)
        return -1;
    cur->node_ptr = ptr;
    return 0;
}

static void cursor_slot_clear_ptr(struct bdb_value_t* v)
{
    v->next_ptr = 0;
    v->prev_ptr = 0;
}

static int cursor_next_key(struct dbcursor_t* cur)
{
    /* slotのポインタをクリアします。(2011/12/08) */
    if (! cur->bdb->datapack_flag)
        cursor_slot_clear_ptr(&cur->slot.u.dp.v);

    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    /* 次のキーに進めます。*/
    if (cur->index+1 < cur->bdb->leaf_cache->leaf.keynum) {
        if (cursor_get_slot(cur, cur->index+1) < 0)
            return -1;
        return 0;
    }

    /* 次のリーフ */
    if (cur->bdb->leaf_cache->leaf.next_ptr == 0) {
        /* end of cursor */
        return NIO_CURSOR_END;
    }
    return cursor_leaf_top(cur, cur->bdb->leaf_cache->leaf.next_ptr);
}

static int cursor_prev_key(struct dbcursor_t* cur)
{
    /* slotのポインタをクリアします。(2011/12/08) */
    if (! cur->bdb->datapack_flag)
        cursor_slot_clear_ptr(&cur->slot.u.dp.v);
    
    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    /* 前のキーに進めます。*/
    if (cur->index-1 >= 0) {
        if (cursor_get_slot(cur, cur->index-1) < 0)
            return -1;
        return 0;
    }

    /* 前のリーフ */
    if (cur->bdb->leaf_cache->leaf.prev_ptr == 0) {
        /* end of cursor */
        return NIO_CURSOR_END;
    }
    return cursor_leaf_bot(cur, cur->bdb->leaf_cache->leaf.prev_ptr);
}

/* 重複索引の場合のみ呼ばれる */
static int cursor_update_value_ptr(struct dbcursor_t* cur, int64 new_ptr)
{
    struct bdb_leaf_key_t* kp;
    int nodesize;

    kp = &cur->bdb->leaf_cache->keydata[cur->index];
    kp->value.u.dp.v_ptr = new_ptr;

    /* update leaf & keybuf */
    nodesize = leaf_put_keydata(cur->bdb,
                                &cur->bdb->leaf_cache->leaf,
                                cur->bdb->leaf_cache->keydata,
                                cur->bdb->leaf_buf);
    cur->bdb->leaf_cache->leaf.nodesize = BDB_LEAF_SIZE + nodesize;

    if (put_leaf_keybuf(cur->bdb, &cur->bdb->leaf_cache->leaf, cur->bdb->leaf_buf) < 0)
        return -1;
    if (update_leaf(cur->bdb, cur->bdb->leaf_cache->leaf.node_ptr, &cur->bdb->leaf_cache->leaf) < 0)
        return -1;
    return 0;
}

/*
 * オープンされているデータベースファイルからキー順アクセスするための
 * カーソルを作成します。
 * キー位置は先頭に位置づけられます。
 *
 * bdb: データベース構造体のポインタ
 *
 * 成功した場合はカーソル構造体のポインタを返します。
 * エラーの場合は NULL を返します。
 */
struct dbcursor_t* bdb_cursor_open(struct bdb_t* bdb)
{
    struct dbcursor_t* cur;

    cur = (struct dbcursor_t*)calloc(1, sizeof(struct dbcursor_t));
    if (cur == NULL) {
        err_write("bdb: bdb_cursor_open() no memory.");
        return NULL;
    }

    CS_START(&bdb->critical_section);

    cur->bdb = bdb;
    cur->node_ptr = 0;
    cur->index = -1;

    if (bdb->leaf_top_ptr != 0) {
        if (cursor_leaf_top(cur, bdb->leaf_top_ptr) < 0) {
            free(cur);
            cur = NULL;
            goto final;
        }
    }

final:
    CS_END(&bdb->critical_section);
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
void bdb_cursor_close(struct dbcursor_t * cur)
{
    if (cur != NULL) {
        leaf_cache_flush(cur->bdb);
        free(cur);
    }
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
int bdb_cursor_next(struct dbcursor_t* cur)
{
    int result = 0;

    if (cur->index < 0)
        return NIO_CURSOR_END;

    CS_START(&cur->bdb->critical_section);

    if (cur->bdb->dupkey_flag) {
        /* 重複キーの場合は次のデータに位置づけます。*/
        if (cur->slot.u.dp.v.next_ptr != 0) {
            cur->slot.u.dp.v_ptr = cur->slot.u.dp.v.next_ptr;
            result = read_value_header(cur->bdb,
                                       cur->slot.u.dp.v_ptr,
                                       &cur->slot.u.dp.v);
            goto final;
        }
    }
    /* 次のキーに進めます。 */
    result = cursor_next_key(cur);

final:
    CS_END(&cur->bdb->critical_section);
    return result;
}

/*
 * カーソルの現在位置を次のキーに進めます。
 * 重複キーの場合でも次のキーに現在位置が移動します。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に移動できた場合はゼロが返されます。
 * カーソルが終わりの場合は NIO_CURSOR_END が返されます。
 * エラーの場合は -1 が返されます。
 */
int bdb_cursor_nextkey(struct dbcursor_t* cur)
{
    int result = 0;
    
    if (cur->index < 0)
        return NIO_CURSOR_END;
    
    CS_START(&cur->bdb->critical_section);

    /* 次のキーに進めます。 */
    result = cursor_next_key(cur);
    
    CS_END(&cur->bdb->critical_section);
    return result;
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
int bdb_cursor_prev(struct dbcursor_t* cur)
{
    int result = 0;

    if (cur->index < 0)
        return NIO_CURSOR_END;
    
    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    if (cur->bdb->leaf_cache->leaf.node_ptr == cur->bdb->leaf_bot_ptr &&
        cur->index >= cur->bdb->leaf_cache->leaf.keynum)
        return NIO_CURSOR_END;

    CS_START(&cur->bdb->critical_section);

    if (cur->bdb->dupkey_flag) {
        /* 重複キーの場合は前のデータに位置づけます。*/
        if (cur->slot.u.dp.v.prev_ptr != 0) {
            cur->slot.u.dp.v_ptr = cur->slot.u.dp.v.prev_ptr;
            result = read_value_header(cur->bdb,
                                       cur->slot.u.dp.v_ptr,
                                       &cur->slot.u.dp.v);
            goto final;
        }
    }
    /* 前のキーに進めます。*/
    result = cursor_prev_key(cur);

    if (cur->bdb->dupkey_flag) {
        /* 2012/11/09 重複索引の最後に位置づけます。*/
        while (cur->slot.u.dp.v.next_ptr != 0) {
            cur->slot.u.dp.v_ptr = cur->slot.u.dp.v.next_ptr;
            read_value_header(cur->bdb,
                              cur->slot.u.dp.v_ptr,
                              &cur->slot.u.dp.v);
        }
    }
final:
    CS_END(&cur->bdb->critical_section);
    return result;
}

/*
 * カーソルの現在位置を前のキーに進めます。
 * 重複キーの場合でも前のキーに現在位置が移動します。
 *
 * cur: カーソル構造体のポインタ
 *
 * 正常に移動できた場合はゼロが返されます。
 * カーソルの現在位置が先頭の場合は NIO_CURSOR_END が返されます。
 * エラーの場合は -1 が返されます。
 */
int bdb_cursor_prevkey(struct dbcursor_t* cur)
{
    int result = 0;

    if (cur->index < 0)
        return NIO_CURSOR_END;
    
    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    if (cur->bdb->leaf_cache->leaf.node_ptr == cur->bdb->leaf_bot_ptr &&
        cur->index >= cur->bdb->leaf_cache->leaf.keynum)
        return NIO_CURSOR_END;
    
    CS_START(&cur->bdb->critical_section);

    /* 前のキーに進めます。*/
    result = cursor_prev_key(cur);
    
    CS_END(&cur->bdb->critical_section);
    return result;
}

static int seek_duplicate_last(struct dbcursor_t* cur)
{
    int result = 0;
    if (cur->bdb->dupkey_flag) {
        /* 重複キーの場合は次のデータに位置づけます。*/
        while (cur->slot.u.dp.v.next_ptr != 0) {
            cur->slot.u.dp.v_ptr = cur->slot.u.dp.v.next_ptr;
            result = read_value_header(cur->bdb,
                                       cur->slot.u.dp.v_ptr,
                                       &cur->slot.u.dp.v);
            if (result < 0)
                break;
        }
    }
    return result;
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
int bdb_cursor_duplicate_last(struct dbcursor_t* cur)
{
    int result;
    
    if (cur->index < 0)
        return NIO_CURSOR_END;
    
    CS_START(&cur->bdb->critical_section);
    result = seek_duplicate_last(cur);
    CS_END(&cur->bdb->critical_section);
    return result;
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
int bdb_cursor_find(struct dbcursor_t* cur, int cond, const void* key, int keysize)
{
    int result = 0;
    int status;

    switch (cond) {
        case BDB_COND_EQ:
        case BDB_COND_GT:
        case BDB_COND_GE:
        case BDB_COND_LT:
        case BDB_COND_LE:
            break;
        default:
            err_write("bdb: bdb_cursor_find() cond error=%d", cond);
            return -1;
    }

    CS_START(&cur->bdb->critical_section);

    status = search_key(cur->bdb, key, keysize, &cur->slot);
    if (status < 0) {
        result = -1;
        goto final;
    }

    if (status == BDB_KEY_NOTFOUND && cur->bdb->leaf_top_ptr == 0) {
        /* NO DATA */
        result = -1;
        goto final;
    }
    
    cur->node_ptr = cur->bdb->leaf_cache->leaf.node_ptr;

    if (cursor_get_slot(cur, cur->slot.index) < 0)
        return -1;

    if (status == BDB_KEY_FOUND) {
        if (cond == BDB_COND_GT) {
            /* 次のキーに進めます。*/
            if (cursor_next_key(cur) != 0)
                result = -1;
        } else if (cond == BDB_COND_LT) {
            /* 前のキーに進めます。*/
            if (cursor_prev_key(cur) != 0)
                result = -1;
        }
        /* 2012/12/19 */
        if (cond == BDB_COND_LT || cond == BDB_COND_LE) {
            /* 重複索引の場合は最後に位置づけます。*/
            seek_duplicate_last(cur);
        }
    } else {
        /* KEY NOT FOUND */
        if (cond == BDB_COND_LT || cond == BDB_COND_LE) {
            /* 2013/12/19 */
            if (cursor_prev_key(cur) != 0) {
                result = -1;
                goto final;
            }
            /* 重複索引の場合は最後に位置づけます。*/
            seek_duplicate_last(cur);
        } else if (cond == BDB_COND_GT || cond == BDB_COND_GE) {
            /* 2011/10/18 - add at end check */
            if (cur->index >= cur->bdb->leaf_cache->leaf.keynum) {
                /* 2011/11/03 - next leaf */
                if (cursor_next_key(cur) != 0)
                    result = -1;
            }
        } else {
            result = -1;
        }
    }

final:
    CS_END(&cur->bdb->critical_section);
    return result;
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
int bdb_cursor_seek(struct dbcursor_t* cur, int pos)
{
    int result = 0;

    CS_START(&cur->bdb->critical_section);

    if (pos == BDB_SEEK_TOP) {
        if (cur->bdb->leaf_top_ptr != 0) {
            if (cursor_leaf_top(cur, cur->bdb->leaf_top_ptr) < 0)
                result = -1;
        } else {
            result = -1;
        }
    } else if (pos == BDB_SEEK_BOTTOM) {
        if (cur->bdb->leaf_bot_ptr != 0) {
            if (cursor_leaf_bot(cur, cur->bdb->leaf_bot_ptr) < 0)
                result = -1;
        } else {
            result = -1;
        }
    } else {
        err_write("bdb: bdb_cursor_seek() pos error=%d", pos);
        result = -1;
    }

    CS_END(&cur->bdb->critical_section);
    return result;
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
int bdb_cursor_key(struct dbcursor_t* cur, void* key, int keysize)
{
    int ksize = -1;
    struct bdb_leaf_key_t* kp;

    if (cur->index < 0) {
        err_write("bdb_cursor_key: current position undefined.");
        return -1;
    }

    CS_START(&cur->bdb->critical_section);
    
    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    kp = &cur->bdb->leaf_cache->keydata[cur->index];
    if (keysize < kp->keysize)
        goto final;
    ksize = kp->keysize;
    memcpy(key, kp->key, kp->keysize);

final:
    CS_END(&cur->bdb->critical_section);
    return ksize;
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
int bdb_cursor_value(struct dbcursor_t* cur, void* val, int valsize)
{
    int vsize = -1;

    if (cur->index < 0) {
        err_write("bdb_cursor_value: current position undefined.");
        return -1;
    }

    if (cur->bdb->datapack_flag) {
        if (valsize < cur->slot.u.pp.valsize)
            return -1;
    } else {
        if (valsize < cur->slot.u.dp.v.valsize)
            return -1;
    }

    CS_START(&cur->bdb->critical_section);

    if (cur->bdb->datapack_flag) {
        memcpy(val, cur->slot.u.pp.val, cur->slot.u.pp.valsize);
        vsize = cur->slot.u.pp.valsize;
    } else {
        mmap_seek(cur->bdb->nio->mmap, cur->slot.u.dp.v_ptr+BDB_VALUE_SIZE);
        if (mmap_read(cur->bdb->nio->mmap, val, cur->slot.u.dp.v.valsize) != cur->slot.u.dp.v.valsize)
            goto final;
        vsize = cur->slot.u.dp.v.valsize;
    }

final:
    CS_END(&cur->bdb->critical_section);
    return vsize;
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
int bdb_cursor_update(struct dbcursor_t* cur, const void* val, int valsize)
{
    int result = 0;

    if (cur->bdb->datapack_flag) {
        if (valsize > BDB_PACK_DATASIZE) {
            err_write("bdb_cursor_update: valsize is too large, less than %d bytes.", BDB_PACK_DATASIZE);
            return -1;
        }
    }
    if (cur->index < 0) {
        err_write("bdb_cursor_update: current position undefined.");
        return -1;
    }

    CS_START(&cur->bdb->critical_section);
    
    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    if (cur->bdb->datapack_flag) {
        if (update_key_value_pack(cur->bdb,
                                  &cur->bdb->leaf_cache->leaf,
                                  &cur->slot,
                                  cur->bdb->leaf_buf,
                                  cur->bdb->leaf_cache->keydata,
                                  val,
                                  valsize) < 0) {
            result = -1;
            goto final;
        }
    } else {
        int64 ptr;

        ptr = update_key_value(cur->bdb, val, valsize, &cur->slot);
        if (ptr < 0) {
            result = -1;
            goto final;
        }
        if (ptr != cur->slot.u.dp.v_ptr) {
            /* 領域が変わった */
            struct bdb_value_t v;

            if (read_value_header(cur->bdb, ptr, &v) < 0) {
                result = -1;
                goto final;
            }
            if (v.prev_ptr == 0) {
                /* leaf-key のポインタを更新 */
                cur->slot.u.dp.v_ptr = ptr;
                if (update_leaf_by_slot(cur->bdb, &cur->bdb->leaf_cache->leaf, &cur->slot) < 0) {
                    result = -1;
                    goto final;
                }
            } else {
                /* prev の next_ptr を更新 */
                if (read_value_header(cur->bdb, v.prev_ptr, &v) < 0) {
                    result = -1;
                    goto final;
                }
                v.next_ptr = ptr;
                if (write_value_header(cur->bdb, v.prev_ptr, &v) < 0) {
                    result = -1;
                    goto final;
                }
            }
        }
    }

final:
    update_filesize(cur->bdb);
    CS_END(&cur->bdb->critical_section);
    return result;
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
int bdb_cursor_delete(struct dbcursor_t* cur)
{
    int result = 0;

    if (cur->index < 0) {
        err_write("bdb_cursor_delete: current position undefined.");
        return -1;
    }
    
    if (leaf_cache_get(cur->bdb, cur->node_ptr) < 0)
        return -1;

    if (cur->slot.u.dp.v.next_ptr == 0 && cur->slot.u.dp.v.prev_ptr == 0) {
        struct bdb_leaf_key_t* kp;
        ushort ksize;
        char key[NIO_MAX_KEYSIZE];

        /* キーを削除 */
        kp = &cur->bdb->leaf_cache->keydata[cur->index];
        ksize = kp->keysize;
        memcpy(key, kp->key, ksize);
        if (bdb_delete(cur->bdb, key, ksize) < 0)
            return -1;
        /* 次のキーに位置づけます。*/
        if (bdb_cursor_find(cur, BDB_COND_GT, key, ksize) < 0) {
            cur->index = -1;
            return 1;
        }
        return 0;
    }

    if (! cur->bdb->dupkey_flag) {
        err_write("bdb: bdb_cursor_delete() cursor is not duplicate key");
        return -1;
    }

    /* 値だけを削除 */
    CS_START(&cur->bdb->critical_section);

    /* 領域を解放します。*/
    if (nio_add_free_list(cur->bdb->nio,
                          cur->slot.u.dp.v_ptr,
                          cur->slot.u.dp.v.areasize) < 0) {
        result = -1;
        goto final;
    }

    if (cur->slot.u.dp.v.next_ptr != 0) {
        struct bdb_value_t v;

        /* リンクをつなぎ変えます。*/
        result = read_value_header(cur->bdb,
                                   cur->slot.u.dp.v.next_ptr,
                                   &v);
        if (result == 0) {
            v.prev_ptr = cur->slot.u.dp.v.prev_ptr;
            result = write_value_header(cur->bdb,
                                        cur->slot.u.dp.v.next_ptr,
                                        &v);
            if (result < 0)
                goto final;
        }
        if (cur->slot.u.dp.v.prev_ptr == 0) {
            /* リーフノードのデータポインタを更新します。*/
            if (cursor_update_value_ptr(cur, cur->slot.u.dp.v.next_ptr) < 0)
                goto final;
        }
        cur->slot.u.dp.v_ptr = cur->slot.u.dp.v.next_ptr;
    } else {
        struct bdb_value_t v;

        /* リンクをつなぎ変えます。*/
        result = read_value_header(cur->bdb,
                                   cur->slot.u.dp.v.prev_ptr,
                                   &v);
        if (result == 0) {
            v.next_ptr = cur->slot.u.dp.v.next_ptr;
            result = write_value_header(cur->bdb,
                                        cur->slot.u.dp.v.prev_ptr,
                                        &v);
            if (result < 0)
                goto final;
        }
        cur->slot.u.dp.v_ptr = cur->slot.u.dp.v.prev_ptr;
    }

    /* slotを最新にする。*/
    if (cursor_get_slot(cur, cur->index) < 0) {
        result = 1;
        goto final;
    }

final:
    update_filesize(cur->bdb);
    CS_END(&cur->bdb->critical_section);
    return result;
}
