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

#include <time.h>
#define API_INTERNAL
#include "nestalib.h"

/*
 * OUTPUT FILE FORMAT:
 *   logout_write(): [DATE TIME] MESSAGE<LF>
 */

static int out_fd = -1;

APIEXPORT void logout_initialize(const char* out_file)
{
    /* ファイルオープン */
    if (out_file != NULL && *out_file != '\0') {
        out_fd = FILE_OPEN(out_file, O_WRONLY|O_APPEND|O_CREAT, CREATE_MODE);
        if (out_fd < 0) {
            fprintf(stderr, "output file can't open [%d]: ", errno);
            perror("");
        }
    }
}

APIEXPORT void logout_finalize()
{
    /* ファイルクローズ */
    if (out_fd >= 0) {
        FILE_CLOSE(out_fd);
        out_fd = -1;
    }
}

static void output(const char* buf)
{
    char outbuf[2048];
    time_t timebuf;
    struct tm now;

    /* 現在時刻の取得 */
    time(&timebuf);
    mt_localtime(&timebuf, &now);

    /* 出力内容の編集 */
    snprintf(outbuf, sizeof(outbuf), "[%d/%02d/%02d %02d:%02d:%02d] %s\n",
             now.tm_year+1900, now.tm_mon+1, now.tm_mday,
             now.tm_hour, now.tm_min, now.tm_sec,
             buf);

    if (out_fd < 0)
        fprintf(stdout, "%s", outbuf);
    else
        FILE_WRITE(out_fd, outbuf, (int)strlen(outbuf));
}

APIEXPORT void logout_write(const char* fmt, ...)
{
    char outbuf[1024];
    va_list argptr;

    va_start(argptr, fmt);
    vsnprintf(outbuf, sizeof(outbuf), fmt, argptr);
    output(outbuf);
}
