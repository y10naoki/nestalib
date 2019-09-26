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

#define DIO_HEADER_SIZE             32
#define DIO_FREEPAGE_SIZE           256
#define DIO_DATAHEADER_SIZE         8

#define DIO_TIMESTAMP_OFFSET        4
#define DIO_FREEPAGE_OFFSET         12
#define DIO_FILEID                  "NKVD"
#define DIO_FREE_PAGEID             0xCCEE
#define DIO_FREEPAGE_NEXT_OFFSET    2
#define DIO_FREEPAGE_COUNT_OFFSET   14
#define DIO_FREEPAGE_ARRAY_OFFSET   16

APIEXPORT struct dio_data_t* dio_open(const char* fname)
{
    int fd;
    struct dio_data_t* btdat;
    char buf[DIO_HEADER_SIZE];
    char fid[4];
    int64 ctime;
    int64 freeptr;

    fd = FILE_OPEN(fname, O_RDWR|O_BINARY);
    if (fd < 0) {
        err_write("dio_open: file can't open: %s.", fname);
        return NULL;
    }
    btdat = (struct dio_data_t*)calloc(1, sizeof(struct dio_data_t));
    if (btdat == NULL) {
        err_write("dio_open: no memory.");
        FILE_CLOSE(fd);
        return NULL;
    }
    btdat->freepage = (struct dio_free_t*)calloc(1, sizeof(struct dio_free_t));
    if (btdat->freepage == NULL) {
        err_write("dio_open: no memory.");
        FILE_CLOSE(fd);
        return NULL;
    }

    btdat->fd = fd;

    /* メモリマップドファイルのオープン */
    btdat->mmap = mmap_open(fd, MMAP_READWRITE, MMAP_AUTO_SIZE);
    if (btdat->mmap == NULL) {
        err_write("dio_open: can't open mmap.");
        free(btdat->freepage);
        free(btdat);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* ヘッダー部の読み込み */
    if (mmap_read(btdat->mmap, buf, DIO_HEADER_SIZE) != DIO_HEADER_SIZE) {
        err_write("dio_open: can't read header.");
        mmap_close(btdat->mmap);
        free(btdat->freepage);
        free(btdat);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* ファイル識別コード */
    memcpy(fid, buf, sizeof(fid));
    if (memcmp(fid, DIO_FILEID, sizeof(fid)) != 0) {
        err_write("dio_open: illegal file.");
        mmap_close(btdat->mmap);
        free(btdat->freepage);
        free(btdat);
        FILE_CLOSE(fd);
        return NULL;
    }

    /* 作成日時 */
    memcpy(&ctime, &buf[DIO_TIMESTAMP_OFFSET], sizeof(ctime));

    /* 空き管理ページポインタ（8バイト）*/
    memcpy(&freeptr, &buf[DIO_FREEPAGE_OFFSET], sizeof(freeptr));
    btdat->freeptr = (long)freeptr;

    return btdat;
}

APIEXPORT int dio_create(const char* fname)
{
    int fd;
    char buf[DIO_HEADER_SIZE];
    int64 ctime;

    fd = FILE_OPEN(fname, O_RDWR|O_CREAT|O_BINARY, CREATE_MODE);
    if (fd < 0) {
        err_write("dio_create: file can't open: %s.", fname);
        return -1;
    }
    FILE_TRUNCATE(fd, 0);

    /* バッファのクリア */
    memset(buf, '\0', DIO_HEADER_SIZE);

    /* ファイル識別コード */
    memcpy(buf, DIO_FILEID, 4);

    /* 作成日時 */
    ctime = system_time();
    memcpy(&buf[DIO_TIMESTAMP_OFFSET], &ctime, sizeof(ctime));

    /* ヘッダー部の書き出し */
    if (FILE_WRITE(fd, buf, DIO_HEADER_SIZE) != DIO_HEADER_SIZE) {
        err_write("dio_create: can't write header.");
        FILE_CLOSE(fd);
        return -1;
    }
    FILE_CLOSE(fd);
    return 0;
}

APIEXPORT void dio_close(struct dio_data_t* btdat)
{
    mmap_close(btdat->mmap);
    FILE_CLOSE(btdat->fd);
    free(btdat->freepage);
    free(btdat);
}

static int read_free_page(struct dio_data_t* btdat, long ptr, struct dio_free_t* fpg)
{
    char buf[DIO_FREEPAGE_SIZE];
    ushort rid;
    int64 nextptr;
    ushort count;
    int i;
    char* p;

    /* 空き管理ページの読み込み */
    mmap_seek(btdat->mmap, ptr);
    if (mmap_read(btdat->mmap, buf, DIO_FREEPAGE_SIZE) != DIO_FREEPAGE_SIZE) {
        err_write("read_free_page: can't read free page.");
        return -1;
    }
    /* データ識別コードのチェック */
    rid = DIO_FREE_PAGEID;
    if (memcmp(buf, &rid, sizeof(rid)) != 0) {
        err_write("read_free_page: illegal record id.");
        return -1;
    }

    fpg->offset = ptr;

    /* 次空きデータ管理ページポインタ(8バイト) */
    memcpy(&nextptr, &buf[DIO_FREEPAGE_NEXT_OFFSET], sizeof(int64));
    fpg->nextptr = (long)nextptr;

    /* 領域数 */
    memcpy(&count, &buf[DIO_FREEPAGE_COUNT_OFFSET], sizeof(ushort));
    fpg->count = count;

    p = &buf[DIO_FREEPAGE_ARRAY_OFFSET];
    for (i = 0; i < fpg->count; i++) {
        int32 size;
        int64 lptr;

        memcpy(&size, p, sizeof(size));
        p += sizeof(size);
        fpg->pagesize[i] = size;

        memcpy(&lptr, p, sizeof(lptr));
        p += sizeof(lptr);
        fpg->dataptr[i] = (long)lptr;
    }
    return 0;
}

static int write_free_page(struct dio_data_t* btdat, struct dio_free_t* fpg)
{
    char buf[DIO_FREEPAGE_SIZE];
    ushort rid;
    int64 nextptr;
    ushort count;
    int i;
    char* p;

    memset(buf, '\0', DIO_FREEPAGE_SIZE);

    /* データ識別コード */
    rid = DIO_FREE_PAGEID;
    memcpy(buf, &rid, sizeof(rid));

    /* 次空きデータ管理ページポインタ */
    nextptr = fpg->nextptr;
    memcpy(&buf[DIO_FREEPAGE_NEXT_OFFSET], &nextptr, sizeof(int64));

    /* 領域数 */
    count = (ushort)fpg->count;
    memcpy(&buf[DIO_FREEPAGE_COUNT_OFFSET], &count, sizeof(ushort));

    p = &buf[DIO_FREEPAGE_ARRAY_OFFSET];
    for (i = 0; i < fpg->count; i++) {
        int32 size;
        int64 lptr;

        size = fpg->pagesize[i];
        memcpy(p, &size, sizeof(size));
        p += sizeof(size);

        lptr = fpg->dataptr[i];
        memcpy(p, &lptr, sizeof(lptr));
        p += sizeof(lptr);
    }

    /* 空き管理ページの書き出し */
    mmap_seek(btdat->mmap, fpg->offset);
    if (mmap_write(btdat->mmap, buf, DIO_FREEPAGE_SIZE) != DIO_FREEPAGE_SIZE) {
        err_write("write_free_page: can't write free page.");
        return -1;
    }
    return 0;
}

static int put_freeptr(struct dio_data_t* btdat, long ptr)
{
    int64 lptr = ptr;

    mmap_seek(btdat->mmap, DIO_FREEPAGE_OFFSET);
    if (mmap_write(btdat->mmap, &lptr, sizeof(int64)) != sizeof(int64)) {
        err_write("put_freeptr: write error.");
        return -1;
    }
    btdat->freeptr = ptr;
    return 0;
}

static long reuse_space(struct dio_data_t* btdat, int size, int* areasize)
{
    long fptr;
    struct dio_free_t* fpg;
    int result = 0;

    fpg = btdat->freepage;

    /* 空き領域管理ページの読み込み */
    fptr = btdat->freeptr;
    while (fptr != 0 && result == 0) {
        result = read_free_page(btdat, fptr, fpg);
        if (result == 0) {
            int i;

            for (i = 0; i < fpg->count; i++) {
                int asize;

                /* 領域サイズは管理情報の領域サイズ(4バイト)と
                データサイズ(4バイト)を減算した値になります。*/
                asize = fpg->pagesize[i] - DIO_DATAHEADER_SIZE;
                if (asize >= size) {
                    long ptr;

                    /* 格納可能な空き領域が見つかった */
                    ptr = fpg->dataptr[i];
                    *areasize = asize;
                    fpg->count--;

                    /* fpg->countがゼロになった場合はファイルの最後の時のみ
                       ファイルサイズを調整する。それ以外は空き管理ページの
                       削除は行わない。
                       再利用できるサイズが小さいためと処理が煩雑になるため */
                    if (fpg->count == 0) {
                        if (btdat->mmap->real_size == fptr+DIO_FREEPAGE_SIZE) {
                            btdat->mmap->real_size = fptr;
                            if (btdat->freeptr == fptr) {
                                /* 空き領域管理ポインタを更新します。*/
                                if (put_freeptr(btdat, fpg->nextptr) < 0)
                                    return -1;
                            }
                            return ptr;
                        }
                    }
                    if (i < fpg->count) {
                        /* 空き領域を左シフトします。*/
                        int shift_n;

                        shift_n = fpg->count - i;
                        memmove(&fpg->pagesize[i], &fpg->pagesize[i+1], shift_n * sizeof(int));
                        memmove(&fpg->dataptr[i], &fpg->dataptr[i+1], shift_n * sizeof(long));
                    }
                    if (write_free_page(btdat, fpg) < 0)
                        return -1;

                    return ptr; /* found space */
                }
            }
            fptr = fpg->nextptr;
        }
    }
    return -1;  /* not found */
}

APIEXPORT long dio_avail_space(struct dio_data_t* btdat, int size)
{
    long ptr = -1;
    int areasize;

    if (btdat->freeptr != 0) {
        /* 空き領域管理ページから再利用できる領域を検索します。*/
        ptr = reuse_space(btdat, size, &areasize);
        if (ptr > 0)
            mmap_seek(btdat->mmap, ptr);
    }

    if (ptr < 0) {
        int pagesize;

        /* 空き領域はないため、ファイルの最後に追加します。*/
        ptr = (long)mmap_seek(btdat->mmap, btdat->mmap->real_size);

        /* データ領域サイズを求めます。（16の倍数）*/
        pagesize = sizeof(int32) + sizeof(int32) + size;
        if (pagesize%16 != 0)
            pagesize = ((pagesize + 16) / 16) * 16;
        areasize = pagesize - (sizeof(int32) + sizeof(int32));
    }

    /* 領域サイズを書き出します。*/
    if (mmap_write(btdat->mmap, &areasize, sizeof(areasize)) != sizeof(areasize)) {
        err_write("dio_avail_space: can't write area size=%d.", areasize);
        return -1;
    }
    /* データサイズを書き出します。*/
    if (mmap_write(btdat->mmap, &size, sizeof(size)) != sizeof(size)) {
        err_write("dio_avail_space: can't write data size=%d.", size);
        return -1;
    }
    return ptr;
}

APIEXPORT int dio_read(struct dio_data_t* btdat, long ptr, void* data, int size)
{
    int areasize;
    int datasize;

    /* 領域サイズを読み込みます。*/
    mmap_seek(btdat->mmap, ptr);
    if (mmap_read(btdat->mmap, &areasize, sizeof(areasize)) != sizeof(areasize)) {
        err_write("dio_read: can't read area size.");
        return -1;
    }
    /* データサイズを読み込みます。*/
    if (mmap_read(btdat->mmap, &datasize, sizeof(datasize)) != sizeof(datasize)) {
        err_write("dio_read: can't read data size.");
        return -1;
    }
    if (size < datasize) {
        /* 読み込みバッファが小さい。*/
        err_write("dio_read: data buffer is small, size=%d.", datasize);
        return -1;
    }
    /* データを読み込みます。*/
    if (mmap_read(btdat->mmap, data, datasize) != datasize) {
        err_write("dio_read: can't read data size=%d.", datasize);
        return -1;
    }
    return 0;
}

APIEXPORT int dio_write(struct dio_data_t* btdat, long ptr, const void* data, int size)
{
    int areasize;
    void* buf;
    size_t wb;

    /* 領域サイズを読み込みます。*/
    mmap_seek(btdat->mmap, ptr);
    if (mmap_read(btdat->mmap, &areasize, sizeof(areasize)) != sizeof(areasize)) {
        err_write("dio_write: can't read area size.");
        return -1;
    }
    if (size > areasize) {
        err_write("dio_write: illegal data size.");
        return -1;
    }
    /* データサイズを書き出します。*/
    if (mmap_write(btdat->mmap, &size, sizeof(size)) != sizeof(size)) {
        err_write("dio_write: can't write data size=%d.", size);
        return -1;
    }

    /* 領域サイズにデータを編集します。*/
    if (areasize > size) {
        buf = calloc(1, areasize);
        if (buf == NULL) {
            err_write("dio_write: no memory");
            return -1;
        }
        memcpy(buf, data, size);
    } else {
        buf = (void*)data;
    }

    /* データを書き出します。*/
    wb = mmap_write(btdat->mmap, buf, areasize);
    if (areasize > size)
        free(buf);
    if (wb != areasize) {
        err_write("dio_write: can't write data.");
        return -1;
    }
    return 0;
}

static int new_freepage(struct dio_data_t* btdat,
                        struct dio_free_t* fpg,
                        int size,
                        long ptr)
{
    long last;

    last = (long)btdat->mmap->real_size;

    fpg->offset = last;
    fpg->count = 1;
    fpg->pagesize[0] = size;
    fpg->dataptr[0] = ptr;
    fpg->nextptr = btdat->freeptr;

    /* ヘッダーの空きポインタを更新します。*/
    return put_freeptr(btdat, last);
}

APIEXPORT int dio_delete(struct dio_data_t* btdat, long ptr)
{
    int result = 0;
    int size;
    struct dio_free_t* fpg;

    /* データの領域サイズを取得します。*/
    size = dio_area_size(btdat, ptr);
    if (size < 0)
        return -1;

    /* 解放するサイズは管理情報の領域サイズ(4バイト)と
       データサイズ(4バイト)を加算した値になります。*/
    size += DIO_DATAHEADER_SIZE;

    /* ファイルの最後を削除する場合はファイルサイズを小さくします。*/
    if (btdat->mmap->real_size == ptr+size) {
        /* ファイルサイズを調整します。*/
        btdat->mmap->real_size = ptr;
        return 0;
    }

    fpg = btdat->freepage;
    if (btdat->freeptr != 0) {
        long fptr;

        /* 空き領域管理ページの読み込み */
        fptr = btdat->freeptr;
        while (fptr != 0 && result == 0) {
            result = read_free_page(btdat, fptr, fpg);
            if (result == 0) {
                if (fpg->count < DIO_FREE_COUNT) {
                    fpg->pagesize[fpg->count] = size;
                    fpg->dataptr[fpg->count] = ptr;
                    fpg->count++;
                    break;
                } else {
                    fptr = fpg->nextptr;
                }
            }
        }
        if (fptr == 0 && result == 0) {
            /* 格納領域がなかったため、新しいページを追加します。*/
            result = new_freepage(btdat, fpg, size, ptr);
        }
    } else {
        /* ファイルの最後に追加します。 */
        result = new_freepage(btdat, fpg, size, ptr);
    }
    if (result == 0) {
        /* 空き領域管理ページの書き出し */
        result = write_free_page(btdat, fpg);
    }
    return result;
}

int dio_data_size(struct dio_data_t* btdat, long ptr)
{
    int datasize = -1;
    long offset;

    offset = ptr + sizeof(int32);
    /* データサイズを読み込みます。*/
    mmap_seek(btdat->mmap, offset);
    if (mmap_read(btdat->mmap, &datasize, sizeof(datasize)) != sizeof(datasize)) {
        err_write("dio_data_size: can't read data size.");
        return -1;
    }
    return datasize;
}

int dio_area_size(struct dio_data_t* btdat, long ptr)
{
    int areasize = -1;

    /* 領域サイズを読み込みます。*/
    mmap_seek(btdat->mmap, ptr);
    if (mmap_read(btdat->mmap, &areasize, sizeof(areasize)) != sizeof(areasize)) {
        err_write("dio_area_size: can't read area size.");
        return -1;
    }
    return areasize;
}
