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
#include "hdb.h"

/* ハッシュ法を用いたデータベースの関数群です。
 * 関数はマルチスレッドで動作します。
 *
 * キーの長さは 1024 バイト以下に制限されています。
 * 重複キーは許されていません。
 *
 * キーはバイナリで比較されます。
 * キー、データともに可変長で扱われます。
 * データ長の最大は31ビット(2GB)まで格納可能です。
 *
 * データベースファイルの拡張子は .hdb になります。
 * エラーが発生した場合はエラーログに出力されます。
 *
 * 参考文献：bit別冊「ファイル構造」(1997)共立出版
 */

#define HDB_FILE_EXT                ".hdb"

/* ヘッダー・ブロック */
#define HDB_HEADER_SIZE             64
#define HDB_FILEID                  "NHSK"
#define HDB_FILE_VERSION            10
#define HDB_TYPE_HASH               0x01

#define HDB_VERSION_OFFSET          4
#define HDB_FILETYPE_OFFSET         6
#define HDB_TIMESTAMP_OFFSET        8
#define HDB_FREEPAGE_OFFSET         16
#define HDB_BUCKETNUM_OFFSET        24
#define HDB_ALIGNMENT_OFFSET        28

/* バケット管理ブロック */
#define HDB_BUCKET_SIZE             16      /* +バケット数ｘ8 */
#define HDB_BUCKET_ID               0xBBEE

/* キーデータ・ブロック */
#define HDB_KEYVALUE_SIZE           32      /* +キーサイズ+値サイズ */

#define HDB_KEYVALUE_ASIZE_OFFSET       0
#define HDB_KEYVALUE_KSIZE_OFFSET       4
#define HDB_KEYVALUE_DSIZE_OFFSET       6
#define HDB_KEYVALUE_NEXT_OFFSET        10
#define HDB_KEYVALUE_TIMESTAMP_OFFSET   18

/* ハッシュ関数用 */
#define HASH_SEED                   1487
#define HASH_FUNC(hdb,k,sz)  ((hdb->hash_func)(k,sz,HASH_SEED) % hdb->bucket_num)

#define DEFAULT_HASH_FUNC           MurmurHash2A
#define DEFAULT_BUCKET_SIZE         1000000

/* key-value構造体 */
struct hdb_keyvalue_t {
    int areasize;                   /* 領域サイズ */
    short keysize;                  /* キーサイズ */
    int valsize;                    /* 値サイズ */
    int64 nextptr;                  /* 次ポインタ（ゼロは終端） */
    int64 timestamp;                /* タイムスタンプ */
};

static void set_default(struct hdb_t* hdb)
{
    hdb->bucket_num = DEFAULT_BUCKET_SIZE;
    hdb->mmap_view_size = MMAP_AUTO_SIZE;
    hdb->align_bytes = 16;      /* キーデータのアラインメント */
    hdb->filling_rate = 10;     /* 空き領域の充填率 */
}

/*
 * データベースオブジェクトを作成します。
 *
 * 戻り値
 *  データベースオブジェクトのポインタを返します。
 *  メモリ不足の場合は NULL を返します。
 */
struct hdb_t* hdb_initialize(struct nio_t* nio)
{
    struct hdb_t* hdb;

    hdb = (struct hdb_t*)calloc(1, sizeof(struct hdb_t));
    if (hdb == NULL) {
        err_write("hdb: no memory.");
        return NULL;
    }
    hdb->nio = nio;

    /* ハッシュ関数の設定 */
    hdb->hash_func = DEFAULT_HASH_FUNC;

    /* キー比較関数の設定 */
    hdb->cmp_func = nio_cmpkey;

    /* プロパティの設定 */
    set_default(hdb);

    /* クリティカルセクションの初期化 */
    CS_INIT(&hdb->critical_section);

    return hdb;
}

/*
 * データベースオブジェクトを解放します。
 * 確保されていた領域が解放されます。
 *
 * hdb: データベースオブジェクトのポインタ
 *
 * 戻り値
 *  なし
 */
void hdb_finalize(struct hdb_t* hdb)
{
    /* クリティカルセクションの削除 */
    CS_DELETE(&hdb->critical_section);

    free(hdb);
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
 * hdb: データベースオブジェクトのポインタ
 * func: 関数のポインタ
 *
 * 戻り値
 *  なし
 */
void hdb_cmpfunc(struct hdb_t* hdb, CMP_FUNCPTR func)
{
    hdb->cmp_func = func;
}

/*
 * ハッシュデータベースで使用するハッシュ関数を設定します。
 *
 * ハッシュ関数のプロトタイプは以下の通りです。
 * unsigned int HASHFUNC(const void* key, int len, unsigned int seed);
 *
 * デフォルトでは MurmurHash2A が使用されます。
 *
 * hdb: データベースオブジェクトのポインタ
 * func: ハッシュ関数のポインタ
 *
 * 戻り値
 *  なし
 */
void hdb_hashfunc(struct hdb_t* hdb, HASH_FUNCPTR func)
{
    hdb->hash_func = func;
}

/*
 * データベースのプロパティを設定します。
 *
 * プロパティ種類：
 *     NIO_BUCKET_NUM    バケット配列数
 *     NIO_MAP_VIEWSIZE  マップサイズ
 *     NIO_ALIGN_BYTES   キーデータ境界サイズ
 *     NIO_FILLING_RATE  データ充填率
 *
 * hdb: データベースオブジェクトのポインタ
 * kind: プロパティ種類
 * value: 値
 *
 * 戻り値
 *  設定した場合はゼロを返します。エラーの場合は -1 を返します。
 */
int hdb_property(struct hdb_t* hdb, int kind, int value)
{
    int result = 0;

    switch (kind) {
        case NIO_BUCKET_NUM:
            hdb->bucket_num = value;
            break;
        case NIO_MAP_VIEWSIZE:
            hdb->mmap_view_size = value * 1024 * 1024;
            break;
        case NIO_ALIGN_BYTES:
            hdb->align_bytes = value;
            break;
        case NIO_FILLING_RATE:
            hdb->filling_rate = value;
            break;
        default:
            result = -1;
            break;
    }
    return result;
}

/*
 * データベースファイルをオープンします。
 *
 * ファイル名には拡張子のないベース名を指定します。
 *
 * hdb: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  オープンできた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int hdb_open(struct hdb_t* hdb, const char* fname)
{
    char fpath[MAX_PATH+1];
    int fd;
    char buf[HDB_HEADER_SIZE];
    char fid[4];
    int64 ctime;
    int64 freeptr;
    int bucket_num;
    ushort align_bytes;
    char bbuf[HDB_BUCKET_SIZE];
    ushort bid;

    if (strlen(fname)+4 > MAX_PATH) {
        err_write("hdb_open: filename is too long.");
        return -1;
    }

    /* ファイル名を作成します。*/
    nio_make_filename(fpath, fname, HDB_FILE_EXT);

    fd = FILE_OPEN(fpath, O_RDWR|O_BINARY);
    if (fd < 0) {
        err_write("hdb_open: file can't open: %s.", fname);
        return -1;
    }
    hdb->fd = fd;

    /* メモリマップドファイルのオープン */
    hdb->nio->mmap = mmap_open(fd, MMAP_READWRITE, hdb->mmap_view_size);
    if (hdb->nio->mmap == NULL) {
        err_write("hdb_open: can't open mmap.");
        FILE_CLOSE(fd);
        return -1;
    }
    /* ヘッダー部の読み込み */
    if (mmap_read(hdb->nio->mmap, buf, HDB_HEADER_SIZE) != HDB_HEADER_SIZE) {
        err_write("hdb_open: can't read header.");
        mmap_close(hdb->nio->mmap);
        FILE_CLOSE(fd);
        return -1;
    }
    /* ファイル識別コード */
    memcpy(fid, buf, sizeof(fid));
    if (memcmp(fid, HDB_FILEID, sizeof(fid)) != 0) {
        err_write("hdb_open: illegal file.");
        mmap_close(hdb->nio->mmap);
        FILE_CLOSE(fd);
        return -1;
    }

    /* 作成日時 */
    memcpy(&ctime, &buf[HDB_TIMESTAMP_OFFSET], sizeof(ctime));
    /* 空き管理ページポインタ（8バイト）*/
    memcpy(&freeptr, &buf[HDB_FREEPAGE_OFFSET], sizeof(freeptr));
    hdb->nio->free_ptr = freeptr;
    /* ハッシュテーブルのバケット数（4バイト） */
    memcpy(&bucket_num, &buf[HDB_BUCKETNUM_OFFSET], sizeof(bucket_num));
    hdb->bucket_num = bucket_num;
    /* キーデータのアラインメント（2バイト） */
    memcpy(&align_bytes, &buf[HDB_ALIGNMENT_OFFSET], sizeof(align_bytes));
    hdb->align_bytes = align_bytes;
    /* バケットデータ部の読み込み */
    if (mmap_read(hdb->nio->mmap, bbuf, HDB_BUCKET_SIZE) != HDB_BUCKET_SIZE) {
        err_write("hdb_open: can't read bucket.");
        mmap_close(hdb->nio->mmap);
        FILE_CLOSE(fd);
        return -1;
    }
    memcpy(&bid, bbuf, sizeof(bid));
    if (bid != HDB_BUCKET_ID) {
        err_write("hdb_open: illegal bucket-id.");
        mmap_close(hdb->nio->mmap);
        FILE_CLOSE(fd);
        return -1;
    }
    return 0;
}

/*
 * データベースファイルを新規に作成します。
 * ファイルがすでに存在する場合でも新規に作成されます。
 *
 * ファイル名には拡張子のないベース名を指定します。
 *
 * hdb: データベースオブジェクトのポインタ
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  オープンできた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int hdb_create(struct hdb_t* hdb, const char* fname)
{
    char fpath[MAX_PATH+1];
    int fd;
    char buf[HDB_HEADER_SIZE];
    ushort fver = HDB_FILE_VERSION;
    ushort ftype = HDB_TYPE_HASH;
    int64 ctime;
    char bbuf[HDB_BUCKET_SIZE];
    ushort bid;
    int64* bucket_array;
    int bucket_size;

    if (strlen(fname)+4 > MAX_PATH) {
        err_write("hdb_create: filename is too long.");
        return -1;
    }

    /* ファイル名を作成します。*/
    nio_make_filename(fpath, fname, HDB_FILE_EXT);

    fd = FILE_OPEN(fpath, O_RDWR|O_CREAT|O_BINARY, CREATE_MODE);
    if (fd < 0) {
        err_write("hdb_create: file can't open: %s.", fname);
        return -1;
    }
    FILE_TRUNCATE(fd, 0);

    /* バッファのクリア */
    memset(buf, '\0', HDB_HEADER_SIZE);

    /* ファイル識別コード */
    memcpy(buf, HDB_FILEID, 4);
    /* ファイルバージョン（2バイト）*/
    memcpy(&buf[HDB_VERSION_OFFSET], &fver, sizeof(fver));
    /* ファイルタイプ（2バイト）*/
    memcpy(&buf[HDB_FILETYPE_OFFSET], &ftype, sizeof(ftype));
    /* 作成日時（8バイト） */
    ctime = system_time();
    memcpy(&buf[HDB_TIMESTAMP_OFFSET], &ctime, sizeof(ctime));
    /* バケット数（4バイト） */
    memcpy(&buf[HDB_BUCKETNUM_OFFSET], &hdb->bucket_num, sizeof(hdb->bucket_num));
    /* アラインメント（2バイト） */
    memcpy(&buf[HDB_ALIGNMENT_OFFSET], &hdb->align_bytes, sizeof(hdb->align_bytes));

    /* ヘッダー部の書き出し */
    if (FILE_WRITE(fd, buf, HDB_HEADER_SIZE) != HDB_HEADER_SIZE) {
        err_write("hdb_create: can't write header.");
        FILE_CLOSE(fd);
        return -1;
    }

    bucket_array = (int64*)calloc(hdb->bucket_num, sizeof(int64));
    if (bucket_array == NULL) {
        FILE_TRUNCATE(fd, 0);
        FILE_CLOSE(fd);
        return -1;
    }

    /* バケット部の書き出し */
    memset(bbuf, '\0', HDB_BUCKET_SIZE);
    bid = HDB_BUCKET_ID;
    memcpy(bbuf, &bid, sizeof(bid));
    if (FILE_WRITE(fd, bbuf, HDB_BUCKET_SIZE) != HDB_BUCKET_SIZE) {
        err_write("hdb_create: can't write bucket-id.");
        free(bucket_array);
        FILE_TRUNCATE(fd, 0);
        FILE_CLOSE(fd);
        return -1;
    }
    bucket_size = hdb->bucket_num * sizeof(int64);
    if (FILE_WRITE(fd, bucket_array, bucket_size) != bucket_size) {
        err_write("hdb_create: can't write bucket array.");
        free(bucket_array);
        FILE_TRUNCATE(fd, 0);
        FILE_CLOSE(fd);
        return -1;
    }
    free(bucket_array);

    /* メモリマップドファイルのオープン */
    hdb->nio->mmap = mmap_open(fd, MMAP_READWRITE, hdb->mmap_view_size);
    if (hdb->nio->mmap == NULL) {
        err_write("hdb_create: can't open mmap.");
        FILE_TRUNCATE(fd, 0);
        FILE_CLOSE(fd);
        return -1;
    }
    hdb->fd = fd;
    return 0;
}

/*
 * データベースファイルをクローズします。
 *
 * hdb: データベースオブジェクトのポインタ
 *
 * 戻り値
 *  なし
 */
void hdb_close(struct hdb_t* hdb)
{
    mmap_close(hdb->nio->mmap);
    FILE_CLOSE(hdb->fd);
}

/*
 * ハッシュデータベースが存在するか調べます。
 *
 * fname: ファイル名のポインタ
 *
 * 戻り値
 *  ハッシュデータベースがが存在する場合は 1 を返します。
 *  存在しない場合はゼロを返します。
 */
int hdb_file(const char* fname)
{
    char fpath[MAX_PATH+1];
    struct stat fstat;

    if (strlen(fname)+4 > MAX_PATH) {
        err_write("hdb_file: filename is too long.");
        return -1;
    }

    /* ファイル名を作成します。*/
    nio_make_filename(fpath, fname, HDB_FILE_EXT);

    /* ファイル情報の取得 */
    if (stat(fpath, &fstat) < 0)
        return 0;

    /* ディレクトリか調べます。*/
    if (S_ISDIR(fstat.st_mode))
        return 0;
    /* ファイルが存在する */
    return 1;
}

static int write_keyvalue(struct hdb_t* hdb,
                          int64 offset,
                          struct hdb_keyvalue_t* kv,
                          const void* key,
                          const void* value)
{
    char buf[HDB_KEYVALUE_SIZE];

    /* key-valueヘッダーを編集します。*/
    mmap_seek(hdb->nio->mmap, offset);
    memset(buf, '\0', HDB_KEYVALUE_SIZE);

    /* 領域サイズ */
    memcpy(&buf[HDB_KEYVALUE_ASIZE_OFFSET], &kv->areasize, sizeof(int));
    /* キーサイズ */
    memcpy(&buf[HDB_KEYVALUE_KSIZE_OFFSET], &kv->keysize, sizeof(short));
    /* データサイズ */
    memcpy(&buf[HDB_KEYVALUE_DSIZE_OFFSET], &kv->valsize, sizeof(int));
    /* 次ポインタ */
    memcpy(&buf[HDB_KEYVALUE_NEXT_OFFSET], &kv->nextptr, sizeof(int64));
    /* タイムスタンプ */
    memcpy(&buf[HDB_KEYVALUE_TIMESTAMP_OFFSET], &kv->timestamp, sizeof(int64));

    /* key-valueヘッダーを書き出します。*/
    if (mmap_write(hdb->nio->mmap, buf, HDB_KEYVALUE_SIZE) != HDB_KEYVALUE_SIZE)
        return -1;

    if (key && kv->keysize > 0) {
        /* キー値を書き出します。*/
        if (mmap_write(hdb->nio->mmap, key, kv->keysize) != kv->keysize)
            return -1;
    }
    if (value && kv->valsize > 0) {
        int rbytes;

        /* 値を書き出します。*/
        if (mmap_write(hdb->nio->mmap, value, kv->valsize) != kv->valsize)
            return -1;
        rbytes = kv->areasize - (HDB_KEYVALUE_SIZE + kv->keysize + kv->valsize);
        if (rbytes > 0) {
            void* abuf;

            /* アライメント領域を書き出します。*/
            abuf = alloca(rbytes);
            memset(abuf, '\0', rbytes);
            if (mmap_write(hdb->nio->mmap, abuf, rbytes) != rbytes)
                return -1;
        }
    }
    return 0;
}

static int read_keyvalue_header(struct hdb_t* hdb,
                                int64 offset,
                                struct hdb_keyvalue_t* kv)
{
    char buf[HDB_KEYVALUE_SIZE];

    /* key-valueヘッダーを読み込みます。*/
    mmap_seek(hdb->nio->mmap, offset);
    if (mmap_read(hdb->nio->mmap, buf, HDB_KEYVALUE_SIZE) != HDB_KEYVALUE_SIZE)
        return -1;

    /* 領域サイズ */
    memcpy(&kv->areasize, &buf[HDB_KEYVALUE_ASIZE_OFFSET], sizeof(int));
    /* キーサイズ */
    memcpy(&kv->keysize, &buf[HDB_KEYVALUE_KSIZE_OFFSET], sizeof(short));
    /* データサイズ */
    memcpy(&kv->valsize, &buf[HDB_KEYVALUE_DSIZE_OFFSET], sizeof(int));
    /* 次ポインタ */
    memcpy(&kv->nextptr, &buf[HDB_KEYVALUE_NEXT_OFFSET], sizeof(int64));
    /* タイムスタンプ */
    memcpy(&kv->timestamp, &buf[HDB_KEYVALUE_TIMESTAMP_OFFSET], sizeof(int64));
    return 0;
}

static int update_bucket(struct hdb_t* hdb, int index, int64 dptr)
{
    int64 offset;

    offset = HDB_HEADER_SIZE + HDB_BUCKET_SIZE + index * sizeof(int64);
    mmap_seek(hdb->nio->mmap, offset);
    if (mmap_write(hdb->nio->mmap, &dptr, sizeof(int64)) != sizeof(int64))
        return -1;
    return 0;
}

static int64 get_bucket(struct hdb_t* hdb, int index)
{
    int64 offset;
    int64 dptr;

    offset = HDB_HEADER_SIZE + HDB_BUCKET_SIZE + index * sizeof(int64);
    mmap_seek(hdb->nio->mmap, offset);
    if (mmap_read(hdb->nio->mmap, &dptr, sizeof(int64)) != sizeof(int64))
        return -1;
    return dptr;
}

static int add_keyvalue(struct hdb_t* hdb,
                        int index,
                        const void* key,
                        int keysize,
                        const void* val,
                        int valsize,
                        int64 cas)
{
    int rsize, areasize;
    int64 bptr, ptr;
    struct hdb_keyvalue_t kv;

    /* key-value を書き出す領域を取得します。*/
    rsize = HDB_KEYVALUE_SIZE + keysize + valsize;
    if (hdb->align_bytes > 0) {
        if (rsize % hdb->align_bytes)
            rsize = (rsize / hdb->align_bytes + 1) * hdb->align_bytes;
    }
    ptr = nio_avail_space(hdb->nio, rsize, &areasize, hdb->filling_rate);
    if (ptr < 0)
        return -1;

    /* key-value を編集します。*/
    memset(&kv, '\0', sizeof(struct hdb_keyvalue_t));
    kv.areasize = areasize;
    kv.keysize = keysize;
    kv.valsize = valsize;
    kv.nextptr = 0;
    kv.timestamp = (cas == 0)? system_time() : cas;

    bptr = get_bucket(hdb, index);
    if (bptr != 0) {
        /* バケットにデータがあるのでリストの先頭に追加します。*/
        kv.nextptr = bptr;
    }

    /* key-value を書き出します。*/
    if (write_keyvalue(hdb, ptr, &kv, key, val) < 0) {
        err_write("add_keyvalue: can't write key-value header.");
        return -1;
    }
    /* バケットの内容を更新します。*/
    if (update_bucket(hdb, index, ptr) < 0) {
        err_write("add_keyvalue: can't update bucket, index=%d", index);
        return -1;
    }
    return 0;
}

static int64 find_key(struct hdb_t* hdb,
                      int64 ptr,
                      const void* key,
                      int keysize,
                      struct hdb_keyvalue_t* kv)
{
    char* tkey;

    if (ptr == 0) {
        /* not found */
        return 0;
    }

    /* 線形リストを順次検索します。*/
    tkey = (char*)alloca(keysize);
    while (ptr != 0) {
        if (read_keyvalue_header(hdb, ptr, kv) < 0) {
            err_write("find_key: can't read key-value, ptr=%ld", ptr);
            return -1;
        }
        if (kv->keysize == keysize) {
            if (mmap_read(hdb->nio->mmap, tkey, keysize) != keysize) {
                err_write("find_key: can't mmap_read");
                return -1;
            }
            if ((*hdb->cmp_func)(tkey, keysize, key, keysize) == 0) {
                /* found */
                return ptr;
            }
        }
        ptr = kv->nextptr;
    }
    return 0;  /* not found */
}

static int remove_chain_keyvalue(struct hdb_t* hdb,
                                 int index,
                                 int64 del_ptr,
                                 struct hdb_keyvalue_t* kv)
{
    int64 ptr;

    ptr = get_bucket(hdb, index);
    if (ptr == del_ptr)
        return update_bucket(hdb, index, kv->nextptr);

    while (ptr != 0) {
        struct hdb_keyvalue_t kvt;

        if (read_keyvalue_header(hdb, ptr, &kvt) < 0) {
            err_write("remove_chain_keyvalue: can't read key-value, ptr=%ld", ptr);
            return -1;
        }
        if (kvt.nextptr == del_ptr) {
            struct hdb_keyvalue_t delkv;

            if (read_keyvalue_header(hdb, del_ptr, &delkv) < 0) {
                err_write("remove_chain_keyvalue: can't read key-value, ptr=%ld", del_ptr);
                return -1;
            }
            kvt.nextptr = delkv.nextptr;
            if (write_keyvalue(hdb, ptr, &kvt, NULL, NULL) < 0) {
                err_write("remove_chain_keyvalue: can't write key-value, ptr=%ld", ptr);
                return -1;
            }
            return 0;
        }
        ptr = kvt.nextptr;
    }
    return -1;  /* notfound */
}

/*
 * データベースからキーを検索して値のサイズを取得します。
 *
 * hdb: ハッシュデータベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 *
 * キー値を存在していた場合は値のサイズを返します。
 * キーが存在しない場合は -1 を返します。
 */
int hdb_find(struct hdb_t* hdb, const void* key, int keysize)
{
    int dsize = 0;
    int hindex;
    struct hdb_keyvalue_t kv;
    int64 bptr, dptr;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("hdb_find: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&hdb->critical_section);

    /* キーのハッシュ値を求めます。*/
    hindex = HASH_FUNC(hdb, key, keysize);

    /* バケット値を取得します。*/
    bptr = get_bucket(hdb, hindex);
    if (bptr == 0) {
        /* not found */
        dsize = -1;
        goto final;
    }

    dptr = find_key(hdb, bptr, key, keysize, &kv);
    if (dptr <= 0) {
        dsize = -1;
        goto final;
    }
    dsize = kv.valsize;

final:
    CS_END(&hdb->critical_section);
    return dsize;
}

/*
 * データベースからキーを検索して値をポインタに設定します。
 *
 * hdb: ハッシュデータベース構造体のポインタ
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
int hdb_get(struct hdb_t* hdb, const void* key, int keysize, void* val, int valsize)
{
    return hdb_gets(hdb, key, keysize, val, valsize, NULL);
}

/*
 * データベースからキーを検索して値をポインタに設定します。
 * 楽観的排他制御を実現するためのユニークな値を取得します。
 *
 * hdb: ハッシュデータベース構造体のポインタ
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
int hdb_gets(struct hdb_t* hdb, const void* key, int keysize, void* val, int valsize, int64* cas)
{
    int dsize = 0;
    int hindex;
    struct hdb_keyvalue_t kv;
    int64 bptr, dptr;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("hdb_gets: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&hdb->critical_section);

    /* キーのハッシュ値を求めます。*/
    hindex = HASH_FUNC(hdb, key, keysize);

    /* バケット値を取得します。*/
    bptr = get_bucket(hdb, hindex);

    if (bptr == 0) {
        /* not found */
        dsize = -1;
        goto final;
    }

    dptr = find_key(hdb, bptr, key, keysize, &kv);
    if (dptr < 0) {
        dsize = -3;
        goto final;
    } else if (dptr == 0) {
        dsize = -1;
        goto final;
    }

    if (kv.valsize > valsize) {
        /* 領域不足 */
        dsize = -2;
    } else {
        if (mmap_read(hdb->nio->mmap, val, kv.valsize) != kv.valsize) {
            err_write("hdb_get: can't mmap_read.");
            return -1;
        }
        dsize = kv.valsize;
        if (cas != NULL)
            *cas = kv.timestamp;
    }

final:
    CS_END(&hdb->critical_section);
    return dsize;
}

/*
 * データベースからキーを検索して値のポインタを返します。
 * 値の領域は関数内で確保されます。
 * 値のポインタは使用後に hdb_free() で解放する必要があります。
 *
 * hdb: ハッシュデータベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * valsize: 値の領域サイズが設定されるポインタ
 *
 * キー値を取得できた場合は値のサイズを設定して領域のポインタを返します。
 * キーが存在しない場合は valsize に -1 が設定されて NULL を返します。
 * その他のエラーの場合は valsize に -2 が設定されて NULL を返します。
 */
void* hdb_aget(struct hdb_t* hdb, const void* key, int keysize, int* valsize)
{
    return hdb_agets(hdb, key, keysize, valsize, NULL);
}

/*
 * データベースからキーを検索して値のポインタを返します。
 * 値の領域は関数内で確保されます。
 * 値のポインタは使用後に hdb_free() で解放する必要があります。
 * 楽観的排他制御を実現するためのユニークな値を取得します。
 *
 * hdb: ハッシュデータベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * valsize: 値の領域サイズが設定されるポインタ
 * cas: 楽観的排他制御のための値のポインタ
 *
 * キー値を取得できた場合は値のサイズを設定して領域のポインタを返します。
 * キーが存在しない場合は valsize に -1 が設定されて NULL を返します。
 * その他のエラーの場合は valsize に -2 が設定されて NULL を返します。
 */
void* hdb_agets(struct hdb_t* hdb, const void* key, int keysize, int* valsize, int64* cas)
{
    void* val = NULL;
    int hindex;
    struct hdb_keyvalue_t kv;
    int64 bptr, dptr;

    *valsize = -2;
    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("hdb_agets: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return NULL;
    }

    CS_START(&hdb->critical_section);

    /* キーのハッシュ値を求めます。*/
    hindex = HASH_FUNC(hdb, key, keysize);

    /* バケット値を取得します。*/
    bptr = get_bucket(hdb, hindex);

    if (bptr == 0) {
        *valsize = -1;
        goto final;     /* not found */
    }
    dptr = find_key(hdb, bptr, key, keysize, &kv);
    if (dptr < 0) {
        goto final;
    } else if (dptr == 0) {
        *valsize = -1;
        goto final;
    }

    val = malloc(kv.valsize);
    if (val == NULL) {
        err_write("hdb_agets: no memory %d bytes.", kv.valsize);
        goto final;
    }

    if (mmap_read(hdb->nio->mmap, val, kv.valsize) != kv.valsize) {
        err_write("hdb_agets: can't mmap_read.");
        free(val);
        val = NULL;
        goto final;
    }
    *valsize = kv.valsize;

    if (cas != NULL)
        *cas = kv.timestamp;

final:
    CS_END(&hdb->critical_section);
    return val;
}

/*
 * データベースにキーと値を設定します。
 * キーがすでに存在している場合は置換されます。
 *
 * hdb: ハッシュデータベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値のサイズ
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int hdb_put(struct hdb_t* hdb, const void* key, int keysize, const void* val, int valsize)
{
    return hdb_puts(hdb, key, keysize, val, valsize, 0);
}

/*
 * データベースにキーと値を設定します。
 * キーがすでに存在している場合は置換されます。
 * データを更新する場合は楽観的排他制御のチェックが行われます。
 *
 * hdb: ハッシュデータベース構造体のポインタ
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
int hdb_puts(struct hdb_t* hdb, const void* key, int keysize, const void* val, int valsize, int64 cas)
{
    int result = 0;
    int hindex;
    struct hdb_keyvalue_t kv;
    int64 bptr, dptr;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("hdb_puts: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&hdb->critical_section);

    /* キーのハッシュ値を求めます。*/
    hindex = HASH_FUNC(hdb, key, keysize);

    /* バケット値を取得します。*/
    bptr = get_bucket(hdb, hindex);

    /* キー値が存在するか調べます。*/
    dptr = find_key(hdb, bptr, key, keysize, &kv);
    if (dptr < 0) {
        result = -1;
        goto final;
    }

    if (dptr == 0) {
        /* 新規に追加します。*/
        if (add_keyvalue(hdb, hindex, key, keysize, val, valsize, 0) < 0)
            result = -1;
    } else {
        if (cas != 0) {
            if (kv.timestamp != cas) {
                err_write("hdb_puts: cas(compare and swap) error.");
                result = -2;
                goto final;
            }
        }
        /* 既存の領域に書き出せるか調べます。*/
        if (kv.areasize >= HDB_KEYVALUE_SIZE + kv.keysize + valsize) {
            /* 値を更新します。*/
            if (mmap_write(hdb->nio->mmap, val, valsize) != valsize) {
                err_write("hdb_put: can't mmap_write.");
                result = -1;
                goto final;
            }
            /* key-valueヘッダーを更新します。*/
            if (kv.valsize != valsize)
                kv.valsize = valsize;
            /* timestampを更新します。*/
            kv.timestamp = system_time();
            if (write_keyvalue(hdb, dptr, &kv, NULL, NULL) < 0) {
                err_write("hdb_put: can't write key-value header.");
                result = -1;
            }
        } else {
            /* 収まらないので新たな領域に書き出します。*/
            /* 元の領域のリンクを切ります。*/
            if (remove_chain_keyvalue(hdb, hindex, dptr, &kv) < 0) {
                result = -1;
                goto final;
            }
            /* 元の領域をフリーリストに登録します。*/
            nio_add_free_list(hdb->nio, dptr, kv.areasize);
            /* ファイルの最後に追加します。*/
            result = add_keyvalue(hdb, hindex, key, keysize, val, valsize, 0);
        }
    }

final:
    CS_END(&hdb->critical_section);
    return result;
}

/*
 * この関数はレプリケーションで使用されます。
 *
 * データベースにキーと値を設定します。
 * キーがすでに存在している場合は置換されます。
 * 楽観的排他制御のための値も更新されます。
 *
 * hdb: ハッシュデータベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 * val: 値のポインタ
 * valsize: 値のサイズ
 * cas: 楽観的排他制御のための値
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int hdb_bset(struct hdb_t* hdb, const void* key, int keysize, const void* val, int valsize, int64 cas)
{
    int result = 0;
    int hindex;
    struct hdb_keyvalue_t kv;
    int64 bptr, dptr;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("hdb_bset: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&hdb->critical_section);

    /* キーのハッシュ値を求めます。*/
    hindex = HASH_FUNC(hdb, key, keysize);

    /* バケット値を取得します。*/
    bptr = get_bucket(hdb, hindex);

    /* キー値が存在するか調べます。*/
    dptr = find_key(hdb, bptr, key, keysize, &kv);
    if (dptr < 0) {
        result = -1;
        goto final;
    }

    if (dptr == 0) {
        /* 新規に追加します。*/
        if (add_keyvalue(hdb, hindex, key, keysize, val, valsize, cas) < 0)
            result = -1;
    } else {
        /* 既存の領域に書き出せるか調べます。*/
        if (kv.areasize >= HDB_KEYVALUE_SIZE + kv.keysize + valsize) {
            /* 値を更新します。*/
            if (mmap_write(hdb->nio->mmap, val, valsize) != valsize) {
                err_write("hdb_bset: can't mmap_write.");
                result = -1;
                goto final;
            }
            /* key-valueヘッダーを更新します。*/
            if (kv.valsize != valsize)
                kv.valsize = valsize;
            /* timestampを更新します。*/
            kv.timestamp = cas;
            if (write_keyvalue(hdb, dptr, &kv, NULL, NULL) < 0) {
                err_write("hdb_put: can't write key-value header.");
                result = -1;
            }
        } else {
            /* 収まらないので新たな領域に書き出します。*/
            /* 元の領域のリンクを切ります。*/
            if (remove_chain_keyvalue(hdb, hindex, dptr, &kv) < 0) {
                result = -1;
                goto final;
            }
            /* 元の領域をフリーリストに登録します。*/
            nio_add_free_list(hdb->nio, dptr, kv.areasize);
            /* ファイルの最後に追加します。*/
            result = add_keyvalue(hdb, hindex, key, keysize, val, valsize, cas);
        }
    }

final:
    CS_END(&hdb->critical_section);
    return result;
}

/*
 * データベースからキーを削除します。
 * 削除された領域は再利用されます。
 *
 * hdb: ハッシュデータベース構造体のポインタ
 * key: キーのポインタ
 * keysize: キーのサイズ
 *
 * 成功した場合はゼロを返します。
 * エラーの場合は -1 を返します。
 */
int hdb_delete(struct hdb_t* hdb, const void* key, int keysize)
{
    int result = 0;
    int hindex;
    struct hdb_keyvalue_t kv;
    int64 bptr, dptr;

    if (keysize > NIO_MAX_KEYSIZE) {
        err_write("hdb_delete: keysize is too large, less than %d bytes.", NIO_MAX_KEYSIZE);
        return -1;
    }

    CS_START(&hdb->critical_section);

    /* キーのハッシュ値を求めます。*/
    hindex = HASH_FUNC(hdb, key, keysize);

    /* バケット値を取得します。*/
    bptr = get_bucket(hdb, hindex);

    /* キー値が存在するか調べます。*/
    dptr = find_key(hdb, bptr, key, keysize, &kv);
    if (dptr > 0) {
        /* 領域のリストを切ります。*/
        if (remove_chain_keyvalue(hdb, hindex, dptr, &kv) == 0) {
            /* 領域をフリーリストに登録します。*/
            nio_add_free_list(hdb->nio, dptr, kv.areasize);
        } else {
            result = -1;
        }
    } else {
        result = -1;
    }

    CS_END(&hdb->critical_section);
    return result;
}

/*
 * 関数内で確保された領域を開放します。
 */
void hdb_free(const void* v)
{
    if (v)
        free((void*)v);
}

static int64 cursor_next_bucket(struct hdbcursor_t* cur)
{
    int i;

    for (i = cur->bucket_index+1; i < cur->hdb->bucket_num; i++) {
        int64 bptr;

        bptr = get_bucket(cur->hdb, i);
        if (bptr > 0) {
            cur->bucket_index = i;
            cur->kvptr = bptr;
            return bptr;
        }
    }
    return 0;
}

static int cursor_get_current(struct hdbcursor_t* cur, void* keybuf)
{
    struct hdb_keyvalue_t kv;

    if (read_keyvalue_header(cur->hdb, cur->kvptr, &kv) < 0) {
        err_write("cursor_get_current: can't read key-value, ptr=%ld", cur->kvptr);
        return -1;
    }

    if (mmap_read(cur->hdb->nio->mmap, keybuf, kv.keysize) != kv.keysize) {
        err_write("cursor_get_current: can't mmap_read");
        return -1;
    }
    return kv.keysize;
}

/*
 * オープンされているデータベースファイルから順次アクセスするための
 * カーソルを作成します。
 * キー位置は先頭に位置づけられます。
 *
 * キー順にアクセスではありません。
 * ハッシュ値順に先頭からアクセスされます。
 *
 * hdb: データベース構造体のポインタ
 *
 * 成功した場合はカーソル構造体のポインタを返します。
 * エラーの場合は NULL を返します。
 */
struct hdbcursor_t* hdb_cursor_open(struct hdb_t* hdb)
{
    struct hdbcursor_t* cur;

    cur = (struct hdbcursor_t*)calloc(1, sizeof(struct hdbcursor_t));
    if (cur == NULL) {
        err_write("hdb: hdb_cursor_open() no memory.");
        return NULL;
    }

    CS_START(&hdb->critical_section);

    cur->hdb = hdb;
    cur->bucket_index = -1;
    cur->kvptr = 0;

    cursor_next_bucket(cur);

    CS_END(&hdb->critical_section);
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
void hdb_cursor_close(struct hdbcursor_t * cur)
{
    if (cur != NULL)
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
int hdb_cursor_next(struct hdbcursor_t* cur)
{
    int result = 0;
    struct hdb_keyvalue_t kv;

    if (cur->kvptr == 0)
        return NIO_CURSOR_END;

    CS_START(&cur->hdb->critical_section);

    /* 次のキーに進めます。 */
    if (read_keyvalue_header(cur->hdb, cur->kvptr, &kv) < 0) {
        err_write("hdb_cursor_next: can't read key-value, ptr=%ld", cur->kvptr);
        result = -1;
        goto final;
    }
    if (kv.nextptr == 0) {
        /* 次のバケット */
        if (cursor_next_bucket(cur) == 0) {
            result = NIO_CURSOR_END;
            goto final;
        }
    } else {
        cur->kvptr = kv.nextptr;
    }

final:
    CS_END(&cur->hdb->critical_section);
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
int hdb_cursor_key(struct hdbcursor_t* cur, void* key, int keysize)
{
    int ksize;
    char keybuf[NIO_MAX_KEYSIZE];

    CS_START(&cur->hdb->critical_section);
    ksize = cursor_get_current(cur, keybuf);
    if (ksize < 0)
        goto final;
    if (keysize < ksize)
        goto final;
    memcpy(key, keybuf, ksize);

final:
    CS_END(&cur->hdb->critical_section);
    return ksize;
}
