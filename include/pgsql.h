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
#ifndef _PGSQL_H_
#define _PGSQL_H_

#ifdef HAVE_PGSQL
#include <libpq-fe.h>

#include "apiexp.h"

#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT PGconn* pg_logon(const char* username, const char* password, const char* dbname, const char* host, const char* port);
APIEXPORT void pg_logoff(PGconn* con);
APIEXPORT PGresult* pg_query(PGconn* con, const char* sql);
APIEXPORT int pg_exec(PGconn* con, const char* sql);
APIEXPORT int pg_trans(PGconn* con);
APIEXPORT int pg_commit(PGconn* con);
APIEXPORT int pg_rollback(PGconn* con);

#ifdef __cplusplus
}
#endif

#endif /* HAVE_PGSQL */
#endif /* _PGSQL_H_ */
