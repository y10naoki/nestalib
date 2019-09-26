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
#ifndef _OCIIO_H_
#define _OCIIO_H_

#ifdef HAVE_ORACLE_OCI
#include <oci.h>

#include "apiexp.h"

struct oci_env_t {
    OCIEnv* envhp;
};

struct oci_conn_t {
    struct oci_env_t* env;
    OCIServer* srvhp;
    OCISvcCtx* svchp;
    OCISession* authp;
    OCIError* errhp;
};

struct oci_stmt_t {
    struct oci_conn_t* con;
    OCIStmt* stmthp;
};

#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct oci_env_t* oci_initialize();
APIEXPORT void oci_finalize(struct oci_env_t* env);
APIEXPORT struct oci_conn_t* oci_logon(struct oci_env_t* env, const char* username, const char* password, const char* dbname);
APIEXPORT void oci_logoff(struct oci_conn_t* con);
APIEXPORT struct oci_stmt_t* oci_stmt(struct oci_conn_t* con);
APIEXPORT void oci_stmt_free(struct oci_stmt_t* stmt);
APIEXPORT int oci_prepare(struct oci_stmt_t* stmt, const char* sql);
APIEXPORT int oci_bind_str(struct oci_stmt_t* stmt, int pos, const char* value, const short* indicater);
APIEXPORT int oci_bind_int(struct oci_stmt_t* stmt, int pos, const int* value, const short* indicater);
APIEXPORT int oci_bindname_str(struct oci_stmt_t* stmt, const char* phname, const char* value, const short* indicater);
APIEXPORT int oci_bindname_int(struct oci_stmt_t* stmt, const char* phname, const int* value, const short* indicater);
APIEXPORT int oci_outbind_str(struct oci_stmt_t* stmt, int pos, char* value, int vlen);
APIEXPORT int oci_outbind_int(struct oci_stmt_t* stmt, int pos, int* value);
APIEXPORT int oci_execute(struct oci_stmt_t* stmt);
APIEXPORT int oci_execute2(struct oci_stmt_t* stmt, int rows, int offset);
APIEXPORT int oci_fetch(struct oci_stmt_t* stmt);
APIEXPORT int oci_fetch2(struct oci_stmt_t* stmt, int rows, short op, int offset);
APIEXPORT int oci_fetch_count(struct oci_stmt_t* stmt);
APIEXPORT void oci_commit(struct oci_conn_t* con);
APIEXPORT void oci_rollback(struct oci_conn_t* con);
APIEXPORT void oci_error(OCIError* errhp, int status, char* err_buf);

#ifdef __cplusplus
}
#endif

#endif /* HAVE_ORACLE_OCI */
#endif /* _OCIIO_H_ */
