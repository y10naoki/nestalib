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

#ifndef _WIN32
#include <sys/mman.h>
#endif

#define API_INTERNAL
#include "nestalib.h"

#ifdef _WIN32
#define HIGHDWORD(v) ((DWORD)(((unsigned __int64)(v) >> 32) & 0xFFFFFFFF))
#define LOWDWORD(v) ((DWORD)(v))
#endif

#define AUTO_EXTEND_SIZE    (8*1024*1024)    /* 8MB */

/*
 * メモリマップドファイルをラップした関数群です。
 * ファイルアクセスに準じた関数も用意しています。
 *
 * マップサイズが MMAP_AUTO_SIZE の場合は追加書き込みに
 * よってマップサイズが自動的に大ききなります。
 * マップサイズの限界に達した場合は自動拡張がオフになって
 * その後の追加書き込みはファイルに行われます。
 *
 * MMAP_AUTO_SIZE の場合でマップできない大きさのファイルを
 * オープンした場合はマップされるサイズが調整されます。
 * この場合は自動拡張(MMAP_AUTO_SIZE)がオフになります。
 * 
 */

static int64 mmap_size(struct mmap_t* map)
{
    if (map->view_size == MMAP_AUTO_SIZE)
        return map->real_size;

    return map->view_size;
}

static int mmap_open_aux(struct mmap_t* map, int fd, int mode, int64 offset)
{
#ifdef _WIN32
    DWORD mode1;
    DWORD mode2;
    DWORD mode3;
    DWORD szhigh = 0;
    DWORD szlow = 0;
    DWORD offhigh;
    DWORD offlow;

    if (mode == MMAP_READONLY) {
        mode1 = GENERIC_READ;
        mode2 = PAGE_READONLY;
        mode3 = FILE_MAP_READ;
    } else if (mode == MMAP_READWRITE) {
        mode1 = GENERIC_READ | GENERIC_WRITE;
        mode2 = PAGE_READWRITE;
        mode3 = FILE_MAP_ALL_ACCESS;
    } else {
        return -1;
    }

    /* _get_osfhandle() で取得したハンドルはクローズする必要はありません。*/ 
    map->hFile = (HANDLE)_get_osfhandle(fd);
    if (map->hFile == INVALID_HANDLE_VALUE)
        return -1;
    map->real_size = _filelengthi64(fd);
    map->size = mmap_size(map);

    if (map->view_size != MMAP_AUTO_SIZE) {
        szhigh = HIGHDWORD(map->size);
        szlow = LOWDWORD(map->size);
    }

    map->hMap = CreateFileMapping(map->hFile, NULL, mode2, szhigh, szlow, NULL);
    if (map->hMap == NULL)
        return -1;

    offhigh = HIGHDWORD(offset);
    offlow = LOWDWORD(offset);

    map->ptr = MapViewOfFile(map->hMap, mode3, offhigh, offlow, (SIZE_T)map->size);
    if (map->ptr == NULL) {
        CloseHandle(map->hMap);
        map->hMap = NULL;
        return -1;
    }
#else
    struct stat stat_buf;
    int oflag;
    int prot;

    if (mode == MMAP_READONLY) {
        oflag = O_RDONLY;
        prot = PROT_READ;
    } else if (mode == MMAP_READWRITE) {
        oflag = O_RDWR;
        prot = PROT_READ | PROT_WRITE;
    } else {
        return -1;
    }

    if (fstat(fd, &stat_buf) != 0)
        return -1;
    map->real_size = stat_buf.st_size;
    map->size = mmap_size(map);

    if (map->size > map->real_size)
        FILE_TRUNCATE(map->fd, map->size);

    map->ptr = mmap(0, map->size, prot, MAP_SHARED, fd, (off_t)offset);
    if (map->ptr == MAP_FAILED)
        return -1;
#endif
    map->view_offset = offset;
    return 0;
}

static void mmap_unmap(struct mmap_t* map)
{
#ifdef _WIN32
    if (map->ptr)
        UnmapViewOfFile(map->ptr);
    if (map->hMap)
        CloseHandle(map->hMap);

    map->ptr = NULL;
    map->hMap = NULL;
#else
    if (map->ptr)
        munmap(map->ptr, (size_t)map->size);
    map->ptr = NULL;
#endif
}

#if 0
static int mmap_remap(struct mmap_t* map, int64 offset)
{
    int64 pgoff;

    /* オフセットをページ境界に合わせます。*/
    pgoff = (offset / map->pgsize) * map->pgsize;
    if (mmap_open_aux(map, map->fd, map->open_mode, pgoff) < 0)
        return -1;
    map->offset = offset - pgoff;
    return 0;
}
#endif

static int mmap_auto_resize(struct mmap_t* map, int64 newsize)
{
    if (newsize > map->real_size) {
        if (newsize > map->size) {
            /* extend mmap */
            if (mmap_resize(map, newsize+AUTO_EXTEND_SIZE))
                return -1;
        }
        map->real_size = newsize;
    }
    return 0;
}

static int mf_write(struct mmap_t* map, const void* data, size_t size, int64 start, int64 last)
{
    if (last <= map->size) {
        memcpy((char*)map->ptr + map->offset, data, size);
    } else {
        if (start >= map->size) {
            FILE_SEEK(map->fd, start, SEEK_SET);
            if (FILE_WRITE(map->fd, data, (int)size) != size)
                return -1;
        } else {
            size_t m, f;

            /* write on mmap */
            m = (size_t)(map->size - start);
            memcpy((char*)map->ptr + map->offset, data, m);
            /* write on file */
            f = (size_t)(last - map->size);
            FILE_SEEK(map->fd, map->view_offset + map->size, SEEK_SET);
            if (FILE_WRITE(map->fd, (char*)data+m, (int)f) != f)
                return -1;
        }
    }
    if (last > map->real_size)
        map->real_size = last;
    return 0;
}

static int mmap_auto_resize_open(struct mmap_t* map, int64 offset)
{
    int64 cur_size;
    int64 map_size = 0;

    cur_size = map->real_size / 2;
    while (cur_size >= AUTO_EXTEND_SIZE) {
        map->view_size = cur_size / map->pgsize * map->pgsize;
        if (mmap_open_aux(map, map->fd, map->open_mode, offset) == 0) {
            map_size = map->view_size;
            break;
        }
        cur_size /= 2;
    }

    if (map_size < 1)
        return -1;

    cur_size = map_size;
    while (1) {
        mmap_unmap(map);
        cur_size += AUTO_EXTEND_SIZE;
        map->view_size = cur_size / map->pgsize * map->pgsize;
        if (mmap_open_aux(map, map->fd, map->open_mode, offset) != 0)
            break;
        map_size = map->view_size;
    }
    map->view_size = map_size;
    return mmap_open_aux(map, map->fd, map->open_mode, offset);
}

/*
 * メモリマップドファイルを作成します。
 *
 * map_size に MMAP_AUTO_SIZE が指定されてファイルサイズが
 * 大きくてファイル全体がマップできない場合はマップできるサイズで
 * 作成されます。この場合は自動拡張は無効になります。
 *
 * fd: オープン済みのファイルディスクリプタ
 * map_mode: マップモード(MMAP_READONLY, MMAP_READWRITE)
 * map_size: マップサイズ（MMAP_AUTO_SIZEの場合はファイル全体）
 *
 * 戻り値
 *  メモリマップ構造体のポインタ
 */
APIEXPORT struct mmap_t* mmap_open(int fd, int map_mode, int64 map_size)
{
    struct mmap_t* map;
    int result;
#ifdef _WIN32
    SYSTEM_INFO si;
#endif

    map = (struct mmap_t*)calloc(1, sizeof(struct mmap_t));
    if (map == NULL) {
        err_write("mmap_open: no memory");
        return NULL;
    }

#ifdef _WIN32
    GetSystemInfo(&si);
    map->pgsize = si.dwAllocationGranularity;
#else
    map->pgsize = getpagesize();
#endif

    map->open_mode = map_mode;
    map->fd = fd;
    if (map_size == MMAP_AUTO_SIZE)
        map->view_size = MMAP_AUTO_SIZE;
    else
        map->view_size = map_size / map->pgsize * map->pgsize;

    result = mmap_open_aux(map, fd, map_mode, 0);
    if (result != 0) {
        if (map_size == MMAP_AUTO_SIZE)
            result = mmap_auto_resize_open(map, 0);
        if (result == 0) {
            logout_write("mmap_open: mmap resize=%lld to %lld", map->real_size, map->view_size);
        } else {
            free(map);
            err_write("mmap_open: can't allocate memory map, size=%lld", map->real_size);
            return NULL;
        }
    }
    return map;
}

/*
 * メモリマップドファイルをクローズします。
 *
 * map: メモリマップ構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void mmap_close(struct mmap_t* map)
{
    if (map) {
        mmap_unmap(map);
        if (map->size != map->real_size)
            FILE_TRUNCATE(map->fd, map->real_size);
        free(map);
    }
}

/*
 * メモリマップの現在位置からサイズ分のメモリ領域のポインタを返します。
 * 返されたポインタからサイズ分の領域にメモリ操作が行えます。
 *
 * マップのビューサイズが指定されている場合はビューの位置が調整されます。
 *
 * map: メモリマップ構造体のポインタ
 * size: バイト数
 *
 * 戻り値
 *  offset のポインタを返します。
 *  NULL の場合はメモリマップドの範囲から外れているので
 *  mmap_read() か mmap_write() を使用して操作します。
 */
APIEXPORT char* mmap_map(struct mmap_t* map, int64 size)
{
    int64 last;

    last = map->view_offset + map->offset + size;
    if (map->view_size == MMAP_AUTO_SIZE) {
        if (mmap_auto_resize(map, last) < 0)
            return NULL;
        return (char*)map->ptr + map->offset;
    }

    if (map->offset < map->view_offset ||
        last > map->view_offset + map->size) {
        /* 現在のマップ範囲外です。*/
        return NULL;
    }
    return (char*)map->ptr + map->offset;
}

/*
 * 任意のメモリマップの位置からサイズ分のメモリ領域のポインタを返します。
 * 返されたポインタからサイズ分の領域にメモリ操作が行えます。
 *
 * NULL が返された場合でもオフセット位置は更新されます。
 *
 * map: メモリマップ構造体のポインタ
 * offset: 先頭からのバイト数
 * size: バイト数
 *
 * 戻り値
 *  offset のポインタを返します。
 *  NULL の場合はメモリマップドの範囲から外れているので
 *  mmap_read() か mmap_write() を使用して操作します。
 */
APIEXPORT char* mmap_mapping(struct mmap_t* map, int64 offset, int64 size)
{
    map->offset = offset;
    return mmap_map(map, size);
}

/*
 * メモリマップの現在位置を移動します。
 *
 * map: メモリマップ構造体のポインタ
 * offset: 先頭からのバイト数
 *
 * 戻り値
 *  現在位置を返します。
 */
APIEXPORT int64 mmap_seek(struct mmap_t* map, int64 offset)
{
    map->offset = offset;
    return offset;
}

/*
 * メモリマップの現在位置からデータを取得します。
 * 現在位置は取得したサイズ分進みます。
 *
 * 読み込む位置がメモリマップ外の場合はファイルから読み込みます。
 *
 * map: メモリマップ構造体のポインタ
 * data: バッファのポインタ
 * size: 取得するバイト数
 *
 * 戻り値
 *  取得したバイト数を返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT size_t mmap_read(struct mmap_t* map, void* data, size_t size)
{
    int64 start, last;

    start = map->view_offset + map->offset;
    last = start + size;
    if (last > map->real_size) {
        /* end of file */
        err_write("mmap_read: over the file size");
        return -1;
    }

    if (last <= map->size) {
        memcpy(data, (char*)map->ptr + map->offset, size);
    } else {
        if (start >= map->size) {
            FILE_SEEK(map->fd, start, SEEK_SET);
            if (FILE_READ(map->fd, data, size) != size)
                return -1;
        } else {
            size_t m, f;

            /* read on mmap */
            m = (size_t)(map->size - start);
            memcpy(data, (char*)map->ptr + map->offset, m);
            /* read on file */
            f = (size_t)(last - map->size);
            FILE_SEEK(map->fd, map->view_offset + map->size, SEEK_SET);
            if (FILE_READ(map->fd, (char*)data+m, f) != f)
                return -1;
        }
    }
    map->offset += size;
    return size;
}

/*
 * メモリマップの現在位置にデータを設定します。
 *
 * メモリマップが自動拡張の場合はメモリマップのサイズを超えてデータを
 * 設定した場合は、メモリマップの大きさが自動的に拡張されます。
 * 自動拡張であってもメモリマップが拡張できない場合はファイル上に
 * データが出力されます。
 * 自動拡張でない場合はファイル上にデータが出力されます。
 * 現在位置は設定したサイズ分進みます。
 *
 * map: メモリマップ構造体のポインタ
 * data: バッファのポインタ
 * size: 設定するバイト数
 *
 * 戻り値
 *  設定したバイト数を返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT size_t mmap_write(struct mmap_t* map, const void* data, size_t size)
{
    int64 start, last;

    start = map->view_offset + map->offset;
    last = start + size;
    if (map->view_size == MMAP_AUTO_SIZE) {
        if (mmap_auto_resize(map, last) < 0) {
            /* サイズ拡張できないため、
               現在のサイズで固定してファイルに書き出します。*/
            map->view_size = map->size;
            if (mf_write(map, data, size, start, last) < 0)
                return -1;
        } else {
            memcpy((char*)map->ptr + map->offset, data, size);
        }
    } else {
        if (mf_write(map, data, size, start, last) < 0)
            return -1;
    }
    map->offset += size;
    return size;
}

/*
 * メモリマップのサイズを変更します。
 *
 * map: メモリマップ構造体のポインタ
 * size: 設定するサイズ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int mmap_resize(struct mmap_t* map, int64 size)
{
    if (map->size != size) {
        int64 cur_size;

        /* save current map size */
        cur_size = map->real_size;
        /* unmap */
        mmap_unmap(map);
        /* resize */
        if (FILE_TRUNCATE(map->fd, size) < 0) {
            err_write("mmap_resize: file truncate error");
            return -1;
        }
        /* reopen */
        if (mmap_open_aux(map, map->fd, map->open_mode, 0) < 0) {
            err_write("mmap_resize: can't resize, new size=%lld", size);
            /* 元に戻します。*/
            if (FILE_TRUNCATE(map->fd, cur_size) < 0) {
                err_write("mmap_resize: file truncate error, size=%lld", cur_size);
                return -1;
            }
            mmap_open_aux(map, map->fd, map->open_mode, 0);
            err_write("mmap_resize: recover mmap size=%lld.", cur_size);
            return -1;
        }
    }
    return 0;
}
