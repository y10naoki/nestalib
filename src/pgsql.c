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

#ifdef HAVE_PGSQL
#define API_INTERNAL
#include "nestalib.h"
#include "pgsql.h"

/*
 * ユーザー名とパスワードで認証してセッションを開始します。
 *
 * username: ユーザー名
 * password: パスワード
 * dbname: データベース名
 * host: Postgresが動作しているホスト名
 * port: Postgresのポート番号
 *
 * 戻り値
 *  コネクションのポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT PGconn* pg_logon(const char* username,
                           const char* password,
                           const char* dbname,
                           const char* host,
                           const char* port)
{
    PGconn* con = NULL;

    /* データベースとの接続を確立する */
    con = PQsetdbLogin(host,
                       port,
                       NULL,
                       NULL,
                       dbname,
                       username,
                       password);
    if (con == NULL) {
        err_write("pg_exec(): connection faild: %s", host);
        return NULL;
    }

    /* バックエンドとの接続確立に成功したかを確認する */
    if (PQstatus(con) != CONNECTION_OK) {
        err_write("pg_exec(): connection faild: %s", PQerrorMessage(con));
        PQfinish(con);
        return NULL;
    }
    return con;
}

/*
 * セッションを切断します。
 *
 * con: コネクションのポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void pg_logoff(PGconn* con)
{
    PQfinish(con);
}

/*
 * バックエンドのサーバーにクエリを送信します。
 *
 * con: コネクションのポインタ
 * sql: 問い合わせを行なうSQL
 *
 * 戻り値
 *  結果セットのポインタを返します。
 *  エラーの場合はNULLを返します。
 */
APIEXPORT PGresult* pg_query(PGconn* con, const char* sql)
{
    PGresult* res;

    res = PQexec(con, sql);
    if (res == NULL) {
        err_write("pg_query(): '%s' query fail %s", sql, PQerrorMessage(con));
        return NULL;
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        /* 結果セットがない */
        err_write("pg_query(): '%s' query fail.", sql);
        PQclear(res);
        return NULL;
    }
    return res;
}

/*
 * バックエンドのサーバーにコマンドを送信します。
 * コマンドは結果セットを生成しないものです。
 *
 * con: コネクションのポインタ
 * sql: コマンド
 *
 * 戻り値
 *  正常に終了した場合はゼロを返します。
 *  エラーの場合 -1 を返します。
 */
APIEXPORT int pg_exec(PGconn* con, const char* sql)
{
    PGresult* res;

    res = PQexec(con, sql);
    if (res == NULL) {
        err_write("pg_exec(): '%s' command fail: %s", sql, PQerrorMessage(con));
        return -1;
    }
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        err_write("pg_exec(): '%s' command fail.", sql);
        PQclear(res);
        return -1;
    }
    PQclear(res);
    return 0;
}

/*
 * トランザクションを開始します。
 *
 * con: コネクションのポインタ
 *
 * 戻り値
 *  正常に終了した場合はゼロを返します。
 *  エラーの場合 -1 を返します。
 */
APIEXPORT int pg_trans(PGconn* con)
{
    return pg_exec(con, "BEGIN");
}

/*
 * トランザクションを確定します。
 *
 * con: コネクションのポインタ
 *
 * 戻り値
 *  正常に終了した場合はゼロを返します。
 *  エラーの場合 -1 を返します。
 */
APIEXPORT int pg_commit(PGconn* con)
{
    return pg_exec(con, "COMMIT");
}

/*
 * トランザクションをキャンセルします。
 *
 * con: コネクションのポインタ
 *
 * 戻り値
 *  正常に終了した場合はゼロを返します。
 *  エラーの場合 -1 を返します。
 */
APIEXPORT int pg_rollback(PGconn* con)
{
    return pg_exec(con, "ROLLBACK");
}
#endif /* HAVE_PGSQL */
