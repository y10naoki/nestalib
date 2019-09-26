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

#ifdef HAVE_ORACLE_OCI
#define API_INTERNAL
#include "nestalib.h"
#include "ociio.h"

/*
 * OCI を使用するために初期化を行ないます。
 *
 * 戻り値
 *  oci_env_t構造体のポインタを返します。
 */
APIEXPORT struct oci_env_t* oci_initialize()
{
    struct oci_env_t* env;

    env = (struct oci_env_t*)calloc(1, sizeof(struct oci_env_t));
    if (env == NULL) {
        err_write("oci_initialize(): No Memory.\n");
        return NULL;
    }

    /* multi thread OCI initialize */
    OCIInitialize((ub4)OCI_THREADED,
                  (dvoid *)0,
                  (dvoid * (*)(dvoid *, size_t))0,
                  (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                  (void (*)(dvoid *, dvoid *))0);

    /* env */
    OCIEnvInit((OCIEnv **)&env->envhp,
               OCI_DEFAULT,
               (size_t)0,
               (dvoid **)0);
    return env;
}

/*
 * OCI の使用を終了します。
 * 確保されたメモリは解放されます。
 *
 * env: oci_env_t構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void oci_finalize(struct oci_env_t* env)
{
    OCIHandleFree((dvoid *)env->envhp, (ub4)OCI_HTYPE_ENV);
    OCITerminate((ub4)OCI_DEFAULT);

    free(env);
}

/*
 * ユーザー名とパスワードで認証してセッションを開始します。
 *
 * env: oci_env_t構造体のポインタ
 * username: ユーザー名
 * password: パスワード
 * dbname: データベース名（tnsnames.oraで定義されている名称）
 *
 * 戻り値
 *  コネクション構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct oci_conn_t* oci_logon(struct oci_env_t* env,
                                       const char* username,
                                       const char* password,
                                       const char* dbname)
{
    struct oci_conn_t* con;
    int dbname_len;
    sword status;

    con = (struct oci_conn_t*)calloc(1, sizeof(struct oci_conn_t));
    if (con == NULL) {
        err_write("oci_login(): No Memory.\n");
        return NULL;
    }
    con->env = env;


    /* error */
    OCIHandleAlloc((dvoid *)env->envhp,
                   (dvoid **)&con->errhp,
                   OCI_HTYPE_ERROR,
                   (size_t)0,
                   (dvoid **)0);

    /* server contexts */
    OCIHandleAlloc((dvoid *)env->envhp,
                   (dvoid **)&con->srvhp,
                   OCI_HTYPE_SERVER,
                   (size_t)0,
                   (dvoid **)0);

    /* service contexts */
    OCIHandleAlloc((dvoid *)env->envhp,
                   (dvoid **)&con->svchp,
                   OCI_HTYPE_SVCCTX,
                   (size_t)0,
                   (dvoid **)0);

    dbname_len = (dbname == NULL)? 0 : strlen(dbname);
    OCIServerAttach(con->srvhp,
                    con->errhp,
                    (text *)dbname,
                    dbname_len,
                    OCI_DEFAULT);

    /* set attribute server context in the service context */
    OCIAttrSet((dvoid *)con->svchp,
               OCI_HTYPE_SVCCTX,
               (dvoid *)con->srvhp,
               (ub4)0,
               OCI_ATTR_SERVER,
               (OCIError *)con->errhp);

    OCIHandleAlloc((dvoid *)env->envhp,
                   (dvoid **)&con->authp,
                   (ub4)OCI_HTYPE_SESSION,
                   (size_t)0,
                   (dvoid **)0);

    OCIAttrSet((dvoid *)con->authp,
               (ub4)OCI_HTYPE_SESSION,
               (dvoid *)username,
               (ub4)strlen((char *)username),
               (ub4)OCI_ATTR_USERNAME,
               con->errhp);

    OCIAttrSet((dvoid *)con->authp,
               (ub4)OCI_HTYPE_SESSION,
               (dvoid *)password,
               (ub4)strlen((char *)password),
               (ub4)OCI_ATTR_PASSWORD,
               con->errhp);

    status = OCISessionBegin(con->svchp,
                             con->errhp,
                             con->authp,
                             OCI_CRED_RDBMS,
                             (ub4)OCI_DEFAULT);
    if (status != OCI_SUCCESS) {
        oci_logoff(con);
        return NULL;
    }

    OCIAttrSet((dvoid *)con->svchp,
               (ub4)OCI_HTYPE_SVCCTX,
               (dvoid *)con->authp,
               (ub4)0,
               (ub4)OCI_ATTR_SESSION,
               con->errhp);

    return con;
}

/*
 * セッションを終了してコネクションを切断します。
 *
 * con: コネクション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void oci_logoff(struct oci_conn_t* con)
{
    /* end of session */
    OCISessionEnd(con->svchp,
                  con->errhp,
                  con->authp,
                  (ub4)OCI_DEFAULT);

    /* Detach from the server */
    OCIServerDetach(con->srvhp,
                    con->errhp,
                    (ub4)OCI_DEFAULT);

    /* Free the handles */
    if (con->authp)
        OCIHandleFree((dvoid *)con->authp, (ub4)OCI_HTYPE_SESSION);
    if (con->svchp)
        OCIHandleFree((dvoid *)con->svchp, (ub4)OCI_HTYPE_SVCCTX);
    if (con->srvhp)
        OCIHandleFree((dvoid *)con->srvhp, (ub4)OCI_HTYPE_SERVER);
    if (con->errhp)
        OCIHandleFree((dvoid *)con->errhp, (ub4)OCI_HTYPE_ERROR);

    free(con);
}

/*
 * SQLを発行するためのステートメントを作成します。
 *
 * con: コネクション構造体のポインタ
 *
 * 戻り値
 *  ステートメント構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT struct oci_stmt_t* oci_stmt(struct oci_conn_t* con)
{
    struct oci_stmt_t* stmt;
    sword status;

    stmt = (struct oci_stmt_t*)calloc(1, sizeof(struct oci_stmt_t));
    if (stmt == NULL) {
        err_write("oci_stmt(): No Memory.\n");
        return NULL;
    }

    stmt->con = con;

    status = OCIHandleAlloc((dvoid *)con->env->envhp,
                            (dvoid **)&stmt->stmthp,
                            OCI_HTYPE_STMT,
                            (size_t)0,
                            (dvoid **)0);
    return stmt;
}

/*
 * ステートメントを解放します。
 *
 * stmt: ステートメント構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void oci_stmt_free(struct oci_stmt_t* stmt)
{
    OCIHandleFree((dvoid *)stmt->stmthp, (ub4)OCI_HTYPE_STMT);
    free(stmt);
}

/*
 * SQLをサーバーに登録します。
 * 変数部分はプレイスホルダとして定義しておきます。
 *
 * 【名前プレイスホルダ】
 * INSERT INTO cdemo81_emp(empno, ename, job, sal, deptno)
 *     VALUES (:empno, :ename, :job, :sal, :deptno)
 *
 * 【番号プレイスホルダ】
 * SELECT dname FROM cdemo81_dept WHERE deptno = :1
 *
 * stmt: ステートメント構造体のポインタ
 * sql: SQL文
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_prepare(struct oci_stmt_t* stmt, const char* sql)
{
    sword status;

    status = OCIStmtPrepare(stmt->stmthp,
                            stmt->con->errhp,
                            (CONST text *)sql,
                            (ub4)strlen(sql),
                            (ub4)OCI_NTV_SYNTAX,
                            (ub4)OCI_DEFAULT);
    return status;
}

static int oci_bind_pos(struct oci_stmt_t* stmt, int pos, const void* value, int vlen, int sqltype, const short* indicater)
{
    OCIBind* bindhp;
    sword status;

    status = OCIBindByPos(stmt->stmthp,
                          &bindhp,
                          stmt->con->errhp,
                          pos,
                          (dvoid *)value,
                          (sword)vlen,
                          (ub2)sqltype,
                          (dvoid *)indicater,
                          (ub2 *)0,
                          (ub2 *)0,
                          (ub4)0,
                          (ub4 *)0,
                          OCI_DEFAULT);
    return status;
}

/*
 * 番号プレイスホルダに文字列変数を割り当てます。
 *
 * stmt: ステートメント構造体のポインタ
 * pos: 1からの番号
 * value: 文字列変数領域のポインタ
 * indicater: インジケータのポインタ
 *            0: そのままの値が設定されます。
 *            -1: NULL値が設定されます。
 *            NULL: そのままの値が設定されます。
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_bind_str(struct oci_stmt_t* stmt, int pos, const char* value, const short* indicater)
{
    return oci_bind_pos(stmt, pos, value, strlen(value)+1, SQLT_STR, indicater);
}

/*
 * 番号プレイスホルダに整数変数を割り当てます。
 *
 * stmt: ステートメント構造体のポインタ
 * pos: 1からの番号
 * value: int変数領域のポインタ
 * indicater: インジケータのポインタ
 *            0: そのままの値が設定されます。
 *            -1: NULL値が設定されます。
 *            NULL: そのままの値が設定されます。
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_bind_int(struct oci_stmt_t* stmt, int pos, const int* value, const short* indicater)
{
    return oci_bind_pos(stmt, pos, value, sizeof(int), SQLT_INT, indicater);
}

static int oci_bind_name(struct oci_stmt_t* stmt, const char* phname, const void* value, int vlen, int sqltype, const short* indicater)
{
    OCIBind* bindhp;
    sword status;

    status = OCIBindByName(stmt->stmthp,
                           &bindhp,
                           stmt->con->errhp,
                           (text *)phname,
                           strlen(phname),
                           (dvoid *)value,
                           (sword)vlen,
                           (ub2)sqltype,
                           (dvoid *)indicater,
                           (ub2 *)0,
                           (ub2 *)0,
                           (ub4)0,
                           (ub4 *)0,
                           OCI_DEFAULT);
    return status;
}

/*
 * 名前プレイスホルダに文字列変数を割り当てます。
 *
 * stmt: ステートメント構造体のポインタ
 * phname: プレイスホルダ名
 * value: 文字列変数領域のポインタ
 * indicater: インジケータのポインタ
 *            0: そのままの値が設定されます。
 *            -1: NULL値が設定されます。
 *            NULL: そのままの値が設定されます。
 *
 *  oci_bindname_str(stmt, ":JOB", job, joblen+1, &job_ind);
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_bindname_str(struct oci_stmt_t* stmt, const char* phname, const char* value, const short* indicater)
{
    return oci_bind_name(stmt, phname, value, strlen(value)+1, SQLT_STR, indicater);
}

/*
 * 名前プレイスホルダにint変数を割り当てます。
 *
 * stmt: ステートメント構造体のポインタ
 * phname: プレイスホルダ名
 * value: int変数領域のポインタ
 * indicater: インジケータのポインタ
 *            0: そのままの値が設定されます。
 *            -1: NULL値が設定されます。
 *            NULL: そのままの値が設定されます。
 *
 *  oci_bindname_int(stmt, ":EMPNO", &empno, NULL);
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_bindname_int(struct oci_stmt_t* stmt, const char* phname, const int* value, const short* indicater)
{
    return oci_bind_name(stmt, phname, value, sizeof(int), SQLT_INT, indicater);
}

/* 出力用プレイスホルダー */
static int oci_define_pos(struct oci_stmt_t* stmt, int pos, void* value, int vlen, int sqltype)
{
    OCIDefine* defhp;
    sword status;

    status = OCIDefineByPos(stmt->stmthp,
                            &defhp,
                            stmt->con->errhp,
                            pos,
                            (dvoid *)value,
                            (sword)vlen,
                            (ub2)sqltype,   /* SQLT_STR / SQLT_INT */
                            (dvoid *)0,
                            (ub2 *)0,
                            (ub2 *)0,
                            OCI_DEFAULT);
    return status;
}

/*
 * SQL結果に文字列変数を割り当てます。
 *
 * stmt: ステートメント構造体のポインタ
 * pos: 1からの番号
 * value: 文字列変数領域のポインタ
 * vlen: 文字列変数領域のサイズ
 *
 *  oci_outbind_str(stmt, 1, dept, deptlen+1);
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_outbind_str(struct oci_stmt_t* stmt, int pos, char* value, int vlen)
{
    return oci_define_pos(stmt, pos, value, vlen, SQLT_STR);
}

/*
 * SQL結果にint変数を割り当てます。
 *
 * stmt: ステートメント構造体のポインタ
 * pos: 1からの番号
 * value: int変数領域のポインタ
 *
 *  oci_outbind_int(stmt, 1, &num);
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_outbind_int(struct oci_stmt_t* stmt, int pos, int* value)
{
    return oci_define_pos(stmt, pos, value, sizeof(int), SQLT_INT);
}

/*
 * SQLを実行します。
 * 入力変数の場合はその時点の変数の領域が参照されます。
 * 出力変数の場合は変数の領域に値が設定されます。
 *
 * stmt: ステートメント構造体のポインタ
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_execute(struct oci_stmt_t* stmt)
{
    return oci_execute2(stmt, 1, 0);
}

/*
 * 配列を変数としてSQLを実行します。
 * 入力変数の場合はその時点の変数の領域が参照されます。
 * 出力変数の場合は変数の領域に値が設定されます。
 *
 * 配列を指定する場合は連続した領域でなければなりません。
 *
 * stmt: ステートメント構造体のポインタ
 * rows: 配列の要素数
 * offset: ゼロからの開始位置
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_execute2(struct oci_stmt_t* stmt, int rows, int offset)
{
    sword status;

    status = OCIStmtExecute(stmt->con->svchp,
                            stmt->stmthp,
                            stmt->con->errhp,
                            (ub4)rows,
                            (ub4)offset,
                            (CONST OCISnapshot *)0,
                            (OCISnapshot *)0,
                            (ub4)OCI_DEFAULT);
    return status;
}

/*
 * 結果セットの現在位置から１行をフェッチします。
 * 出力変数の領域に値が設定されます。
 *
 * stmt: ステートメント構造体のポインタ
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  データがない場合は OCI_NO_DATA を返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_fetch(struct oci_stmt_t* stmt)
{
    return oci_fetch2(stmt, 1, OCI_FETCH_NEXT, 0);
}

/*
 * 結果セットの現在位置から行をフェッチします。
 * 出力変数の領域に値が設定されます。
 *
 * stmt: ステートメント構造体のポインタ
 * rows: 現行の位置からフェッチされる行数
 * op: 操作方法
 *      OCI_FETCH_CURRENT:  現在行を取得します。
 *      OCI_FETCH_NEXT:     現行位置の次の行を取得します。
 *      OCI_FETCH_FIRST:    結果セットの最初の行を取得します。
 *      OCI_FETCH_LAST:     結果セットの最後の行を取得します。
 *      OCI_FETCH_PRIOR:    結果セットの現在行の前の行に結果セットを位置指定します。このモードを使用して、前の行からも複数の行をフェッチできます。
 *      OCI_FETCH_ABSOLUTE: 絶対的な位置指定を使用して結果セットの行番号（offsetパラメータで指定）をフェッチします。
 *      OCI_FETCH_RELATIVE: 相対的な位置指定を使用して結果セットの行番号（offsetパラメータで指定）をフェッチします。
 * offset: １からの行番号（通常はゼロを指定します）
 *
 * 戻り値
 *  正常終了の場合はゼロを返します。
 *  データがない場合は OCI_NO_DATA を返します。
 *  エラーの場合はゼロ以外の値を返します。
 */
APIEXPORT int oci_fetch2(struct oci_stmt_t* stmt, int rows, short op, int offset)
{
    sword status;

    status = OCIStmtFetch2(stmt->stmthp,
                           stmt->con->errhp,
                           (ub4)rows,
                           (ub2)op,
                           (sb4)offset,
                           (ub4)OCI_DEFAULT);
    return status;
}

/*
 * 結果セットからフェッチした件数を取得します。
 *
 * stmt: ステートメント構造体のポインタ
 *
 * 戻り値
 *  フェッチした件数を返します。
 */
APIEXPORT int oci_fetch_count(struct oci_stmt_t* stmt)
{
    int rows;

    OCIAttrGet(stmt->stmthp,
               OCI_HTYPE_STMT,
               (dvoid *)&rows, 
               (ub4)0,
               OCI_ATTR_ROW_COUNT,
               stmt->con->errhp);
    return rows;
}

/*
 * トランザクションをコミットします。
 *
 * con: コネクション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void oci_commit(struct oci_conn_t* con)
{
   OCITransCommit(con->svchp, con->errhp, (ub4)0);
}

/*
 * トランザクションをロールバックします。
 *
 * con: コネクション構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void oci_rollback(struct oci_conn_t* con)
{
   OCITransRollback(con->svchp, con->errhp, (ub4)0);
}

/* err_buf には 512バイトの領域が必要です。
 */
APIEXPORT void oci_error(OCIError* errhp, int status, char* err_buf)
{
    text errbuf[512];
    sb4 errcode = 0;

    *err_buf = '\0';

    switch (status) {
        case OCI_SUCCESS:
            break;
        case OCI_SUCCESS_WITH_INFO:
            strcpy(err_buf, "OCI_SUCCESS_WITH_INFO");
            break;
        case OCI_NEED_DATA:
            strcpy(err_buf, "OCI_NEED_DATA");
            break;
        case OCI_NO_DATA:
            strcpy(err_buf, "OCI_NODATA");
            break;
        case OCI_ERROR:
            OCIErrorGet((dvoid *)errhp,
                        (ub4)1,
                        (text *)NULL,
                        &errcode,
                        errbuf,
                        (ub4)sizeof(errbuf),
                        OCI_HTYPE_ERROR);
            strcpy(err_buf, (const char*)errbuf);
            break;
        case OCI_INVALID_HANDLE:
            strcpy(err_buf, "OCI_INVALID_HANDLE");
            break;
        case OCI_STILL_EXECUTING:
            strcpy(err_buf, "OCI_STILL_EXECUTE");
            break;
        case OCI_CONTINUE:
            strcpy(err_buf, "OCI_CONTINUE");
            break;
        default:
            break;
    }
}
#endif /* HAVE_ORACLE_OCI */
