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

#include <iconv.h>
#define API_INTERNAL
#include "template.h"
#include "strutil.h"

/*
 * テンプレートを処理する関数群です。
 *
 * テンプレートの文字列は挿入・削除が行ないやすいようにリンクリストで管理されています。
 * 文字列を管理する tpl_object_t構造体の value はプレイスホルダが指定されている場合は
 * 置き換えが発生するため単独のメモリ領域を使用します。
 * プレイスホルダが指定されていない場合は tpl_object_t構造体と連続した領域が使用されます。
 *
 * tpl_value_t構造体はプレイスホルダを置換するための文字列が記憶されます。
 * この構造体が動的に確保されるときに value の領域も連続したメモリで確保されます。
 *
 * tpl_array_t構造体は %repeatコマンドのプレイスホルダを置換するための
 * 文字列配列が記憶されます。
 * この構造体が動的に確保されるときに配列の領域も連続したメモリで確保されます。
 *
 * tpl_erase_t構造体は %eraseコマンドのプレイスホルダを置換するための
 * 文字列配列が記憶されます。
 *
 * テンプレートコマンドは HTMLタグのコメントとして記述します。
 * <!--%コマンド [オプション]-->
 *
 *    %include テンプレートファイル名
 *        %includeコマンドは読み込みに処理されるのでテンプレートファイル名に
 *        プレイスホルダは指定できません。
 *
 *    %replace 検索文字列 置換文字列
 *        検索文字列と置換文字列には {$プレイスホルダ名} が使用できます。
 *        置換処理は %replace コマンドが記述されている行以降に作用します。
 *
 *    %repeat
 *        繰り返しを宣言します。%repeatの次の行から %repeat-end までが繰り返されます。
 *        繰り返される行数はプレイスホルダの配列要素数で決定されます。
 *        配列の要素数が同じでない場合は最大数が適応され、足らない行には
 *        空値が設定されます。
 *
 *    %repeat-end
 *        %repeatコマンドの終了を示します。
 *
 *    %erase {$プレイスホルダ名}
 *        プレイスホルダの値が真（ゼロ以外）の場合は %erase-end までが削除されます。
 *        偽（ゼロの場合）はテンプレートの内容がそのまま出力されます。
 *
 *    %erase-end
 *        %eraseコマンドの終了を示します。
 */
#define R_BUF_SIZE         1024
#define PLACE_HOLDER_SMARK "{$"
#define PLACE_HOLDER_EMARK "}"

/* プレイスホルダ名を作成します。*/
static char* place_holder(const char* name, char* phname, int phsize)
{
    snprintf(phname, phsize, "%s%s%s", PLACE_HOLDER_SMARK, name, PLACE_HOLDER_EMARK);
    return phname;
}

/* テンプレートコマンド行か調べます。
   コマンド名の場合はコマンド名の先頭ポインタを返します。*/
static char* is_command(char* src)
{
    int index;
    char* p;

    index = indexofstr(src, "<!--");
    if (index < 0)
        return NULL;  /* not command */

    src += index + strlen("<!--");
    p = skipsp(src);
    if (*p == '%') {
        p++;
        return p;
    }
    return NULL;
}

static char* get_command(const char* s, char* cmd)
{
    int index;
    int i;

    index = indexof(s, ' ');
    if (index < 0)
        index = indexofstr(s, "-->");
    if (index < 0)
        index = indexofstr(s, PLACE_HOLDER_SMARK);
    for (i = 0; i < index; i++)
        *cmd++ = *s++;
    if (index >= 0)
        *cmd = '\0';
    return cmd;
}

static void delete_object_node(struct tpl_object_t* o, struct tpl_object_t* end)
{
    while (o) {
        struct tpl_object_t* next;

        if (o->attr & ATTR_PLACEHOLDER) {
            if (o->value)
                free(o->value);
        }
        next = o->next;
        free(o);
        if (end != NULL) {
            if (o == end)
                break;
        }
        o = next;
    }
}

static void delete_value_node(struct tpl_value_t* v)
{
    while (v) {
        struct tpl_value_t* next;

        next = v->next;
        free(v);
        v = next;
    }
}

static void delete_array_node(struct tpl_array_t* v)
{
    while (v) {
        struct tpl_array_t* next;

        next = v->next;
        free(v);
        v = next;
    }
}

static void delete_erase_node(struct tpl_erase_t* v)
{
    while (v) {
        struct tpl_erase_t* next;

        next = v->next;
        free(v);
        v = next;
    }
}

/* テンプレートファイルを読んでメモリに設定します。*/
static int topen(struct template_t* tpl,
                 const char* dir_name,
                 const char* file_name)
{
    char fpath[MAX_PATH];
    FILE *fp;
    char buf[R_BUF_SIZE];

    /* フルパスのファイル名を生成します。*/
    snprintf(fpath, sizeof(fpath), "%s/%s", dir_name, file_name);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    /* ファイルの最終更新日時を取得しておきます。*/
    if (stat(fpath, &tpl->file_stat) < 0) {
        err_write("template: file can't stat: %s", fpath);
        return -1;
    }

    /* テンプレートファイルをオープンします。*/
    if ((fp = fopen(fpath, "r")) == NULL) {
        err_write("template: file can't open: %s", fpath);
        return -1;
    }

    /* メモリに読み込みます。*/
    while (fgets(buf, sizeof(buf), fp) != NULL) {
        int index;
        char* p;
        struct tpl_object_t* o;
        unsigned int attr;
        int size;

        /* テンプレートコマンド行か調べます。*/
        attr = 0;
        p = is_command(buf);
        if (p) {
            char* cmd;

            /* コマンド名を取り出します。*/
            cmd = (char*)alloca(strlen(p)+1);
            get_command(p, cmd);

            if (strcmp(cmd, "include") == 0) {
                char inc_fname[MAX_PATH];

                /* %include コマンドの処理を再帰的に行います。*/
                p += strlen("include");
                p = skipsp(p);
                index = indexofstr(p, "-->");
                substr(inc_fname, p, 0, index);
                trim(inc_fname);
                topen(tpl, tpl->dir_name, inc_fname);
                continue;
            } else if (strcmp(cmd, "replace") == 0) {
                attr = ATTR_CMD_REPLACE;
                tpl->replace_flag = 1;
            } else if (strcmp(cmd, "repeat") == 0) {
                attr = ATTR_CMD_REPEAT;
                tpl->repeat_flag = 1;
            } else if (strcmp(cmd, "erase") == 0) {
                attr = ATTR_CMD_ERASE;
                tpl->erase_flag = 1;
            } else if (strcmp(cmd, "repeat-end") == 0) {
                attr = ATTR_CMD_REPEAT_END;
            } else if (strcmp(cmd, "erase-end") == 0) {
                attr = ATTR_CMD_ERASE_END;
            }
        }

        /* プレイスホルダが含まれているか調べます。*/
        if (strstr(buf, PLACE_HOLDER_SMARK) && strstr(buf, PLACE_HOLDER_EMARK))
            attr |= ATTR_PLACEHOLDER;

        /* メモリ確保サイズを決定します。
           プレイスホルダが指定されている場合だけ別々の領域とします。*/
        size = sizeof(struct tpl_object_t);
        if (! (attr & ATTR_PLACEHOLDER))
            size += strlen(buf) + 1;

        /* ノードを作成します。*/
        o = (struct tpl_object_t*)malloc(size);
        if (o == NULL) {
            err_write("template: no memory: %s", fpath);
            fclose(fp);
            return -1;
        }

        if (attr & ATTR_PLACEHOLDER) {
            /* プレイスホルダが指定されている場合は書き換わるため別領域とします。*/
            o->value = (char*)malloc(strlen(buf)+1);
            if (o->value == NULL) {
                err_write("template: no memory: %s", fpath);
                free(o);
                fclose(fp);
                return -1;
            }
        } else {
            char* tp;

            /* 連続したメモリ領域を使用します。*/
            tp = (char*)o;
            o->value = tp + sizeof(struct tpl_object_t);
        }

        o->attr = attr;
        strcpy(o->value, buf);
        o->next = NULL;

        /* リンクリストの最後に追加します。*/
        if (tpl->obj_list == NULL)
            tpl->obj_list = o;
        else
            tpl->last_obj->next = o;

        tpl->last_obj = o;

        /* ファイルサイズに加算します。*/
        tpl->file_size += strlen(buf);
    }
    fclose(fp);
    return 0;
}

/*
 * テンプレートファイルのオープンを行ないます。
 * データを管理するためのメモリを確保します。
 *
 * daemon として起動している場合はカレントディレクトリがないため、
 * dir_name はフルパスで指定します。
 *
 * %include の処理もこのオープン関数で行われます。
 *
 * dir_name: テンプレートファイルのディレクトリ名
 * file_name: テンプレートファイル名
 * encoding: テンプレートファイルのエンコーディング名
 *
 * 戻り値
 *  テンプレート構造体のポインタ
 */
APIEXPORT struct template_t* tpl_open(const char* dir_name,
                                      const char* file_name,
                                      const char* encoding)
{
    struct template_t* tpl;

    /* テンプレート構造体を確保します。*/
    tpl = (struct template_t*)calloc(1, sizeof(struct template_t));
    if (tpl == NULL) {
        err_write("template: no memory.");
        return NULL;
    }
    strncpy(tpl->dir_name, dir_name, sizeof(tpl->dir_name)-1);
    strncpy(tpl->file_name, file_name, sizeof(tpl->file_name)-1);
    if (encoding != NULL)
        strncpy(tpl->file_enc, encoding, sizeof(tpl->file_enc)-1);

    /* クリティカルセクションの初期化 */
    CS_INIT(&tpl->critical_section);

    /* テンプレートファイルを再帰的にメモリに読み込みます。*/
    if (topen(tpl, dir_name, file_name) < 0) {
        tpl_close(tpl);
        return NULL;
    }
    return tpl;
}

/*
 * テンプレートファイルを再オープンします。
 * データを管理するためのメモリを確保します。
 *
 * 再オープンされたテンプレート構造体は tpl_close()でクローズします。
 *
 * テンプレートファイルが更新されていない場合はメモリの内容が新たな構造体にコピーされます。
 * テンプレートファイルが更新されている場合はファイルから構造体が作成されます。
 *
 * 値ノード・配列ノード・消去ノードはクリアされます。
 *
 * tpl: テンプレート構造体のポインタ
 *
 * 戻り値
 *  コピーされたテンプレート構造体のポインタ
 */
APIEXPORT struct template_t* tpl_reopen(struct template_t* tpl)
{
    char fpath[MAX_PATH];
    struct stat fstat;
    struct template_t* tpl2;
    struct tpl_object_t* src;
    struct tpl_object_t* o;

    /* フルパスのファイル名を生成します。*/
    snprintf(fpath, sizeof(fpath), "%s/%s", tpl->dir_name, tpl->file_name);
#ifdef _WIN32
    /* パス区切り文字を置換する */
    chrep(fpath, '/', '\\');
#endif

    /* 変更されている可能性があるためテンプレートファイルの更新日時を取得します。*/
    if (stat(fpath, &fstat) < 0) {
        err_write("template: can't file stat: %s", fpath);
        return NULL;
    }

    /* 更新されているか、テンプレート処理が行なわれている場合はソースを再度読み込みます。*/
    if (tpl->file_stat.st_mtime != fstat.st_mtime ||
        tpl->out_data != NULL) {
        int status;

        /* テンプレートファイルをロックします。*/
        CS_START(&tpl->critical_section);

        /* テンプレートファイルのノードを削除します。*/
        delete_object_node(tpl->obj_list, NULL);
        tpl->obj_list = NULL;

        /* 更新されたテンプレートファイルを再帰的にメモリに読み込みます。*/
        status = topen(tpl, tpl->dir_name, tpl->file_name);

        /* テンプレートファイルのロックを解除します。*/
        CS_END(&tpl->critical_section);

        if (status < 0)
            return NULL;
    }

    /* テンプレート構造体を確保します。*/
    tpl2 = (struct template_t*)calloc(1, sizeof(struct template_t));
    if (tpl == NULL) {
        err_write("template: no memory.");
        return NULL;
    }

    /* テンプレートファイル構造体からコピーします。*/
    strncpy(tpl2->dir_name, tpl->dir_name, sizeof(tpl2->dir_name)-1);
    strncpy(tpl2->file_name, tpl->file_name, sizeof(tpl2->file_name)-1);
    if (strlen(tpl->file_enc) > 0)
        strncpy(tpl2->file_enc, tpl->file_enc, sizeof(tpl2->file_enc)-1);
    tpl2->file_size = tpl->file_size;
    tpl2->replace_flag = tpl->replace_flag;
    tpl2->repeat_flag = tpl->repeat_flag;
    tpl2->erase_flag = tpl->erase_flag;

    /* 元のオブジェクトリストから文字列をコピーします。*/
    src = tpl->obj_list;
    while (src) {
        int size;

        size = sizeof(struct tpl_object_t);
        if (! (src->attr & ATTR_PLACEHOLDER))
            size += strlen(src->value) + 1;

        /* ノードを作成します。*/
        o = (struct tpl_object_t*)malloc(size);
        if (o == NULL) {
            err_write("template: no memory");
            delete_object_node(tpl2->obj_list, NULL);
            free(tpl2);
            return NULL;
        }

        if (src->attr & ATTR_PLACEHOLDER) {
            /* プレイスホルダが指定されている場合は書き換わるため別領域とします。*/
            o->value = (char*)malloc(strlen(src->value)+1);
            if (o->value == NULL) {
                err_write("template: no memory");
                free(o);
                delete_object_node(tpl2->obj_list, NULL);
                free(tpl2);
                return NULL;
            }
        } else {
            char* tp;

            /* 連続したメモリ領域を使用します。*/
            tp = (char*)o;
            o->value = tp + sizeof(struct tpl_object_t);
        }

        o->attr = src->attr;
        strcpy(o->value, src->value);
        o->next = NULL;

        /* リンクリストの最後に追加します。*/
        if (tpl2->obj_list == NULL)
            tpl2->obj_list = o;
        else
            tpl2->last_obj->next = o;

        tpl2->last_obj = o;
        src = src->next;
    }
    return tpl2;
}

/* プレースホルダ名が存在するか調べます。*/
static char* find_place_holder(struct template_t* tpl,
                               const char* phname)
{
    char ph[MAX_PHNAME];
    struct tpl_object_t* o;

    place_holder(phname, ph, sizeof(ph));

    /* リストから文字列を検索します。*/
    o = tpl->obj_list;
    while (o) {
        if (o->attr & ATTR_PLACEHOLDER) {
            if (strstr(o->value, ph))
                return o->value;
        }
        o = o->next;
    }
    return NULL;  /* not found */
}

/* 値ノードの最後を返します。*/
static struct tpl_value_t* last_node_value(struct template_t* tpl)
{
    struct tpl_value_t* v;

    if (tpl->value_list == NULL)
        return NULL;
    v = tpl->value_list;
    while (v->next) {
        v = v->next;
    }
    return v;   /* last node */
}

/* 配列ノードの最後を返します。*/
static struct tpl_array_t* last_node_array(struct template_t* tpl)
{
    struct tpl_array_t* v;

    if (tpl->array_list == NULL)
        return NULL;
    v = tpl->array_list;
    while (v->next) {
        v = v->next;
    }
    return v;   /* last node */
}

/* 消去ノードの最後を返します。*/
static struct tpl_erase_t* last_node_erase(struct template_t* tpl)
{
    struct tpl_erase_t* v;

    if (tpl->erase_list == NULL)
        return NULL;
    v = tpl->erase_list;
    while (v->next) {
        v = v->next;
    }
    return v;   /* last node */
}

/*
 * テンプレートのプレイスホルダを文字列で置換します。
 * 実際の置換処理は tpl_render() が呼び出された時点で行われます。
 *
 * tpl: テンプレート構造体のポインタ
 * phname: プレイスホルダ名
 * value: 置換文字列
 *
 * 戻り値
 *  置換できた場合はゼロを返します。
 *  プレイスホルダ名が見つからない場合は -1 を返します。
 */
APIEXPORT int tpl_set_value(struct template_t* tpl,
                            const char* phname,
                            const char* value)
{
    struct tpl_value_t* v;
    struct tpl_value_t* last;

    if (strlen(phname) > MAX_PHNAME) {
        err_write("template: place holder name too long: %s", phname);
        return -1;
    }

    /* プレイスホルダ名が存在するか調べます。*/
    if (find_place_holder(tpl, phname) == NULL)
        return -1;  /* not found */

    v = (struct tpl_value_t*)malloc(sizeof(struct tpl_value_t) + strlen(value) + 1);
    if (v == NULL) {
        err_write("template: no memory, tpl_set_value()");
        return -1;
    }
    v->value = (char*)v + sizeof(struct tpl_value_t);

    strcpy(v->name, phname);
    strcpy(v->value, value);
    v->next = NULL;

    /* リンクリストの最後に追加します。*/
    last = last_node_value(tpl);
    if (last == NULL)
        tpl->value_list = v;
    else
        last->next = v;
    return 0;
}

/*
 * テンプレートのプレイスホルダを文字列配列で置換します。
 * 実際の置換処理は tpl_render() が呼び出された時点で行われます。
 *
 * 置換文字列配列は、行数 x 列サイズの連続した領域を指定します。
 *
 * tpl: テンプレート構造体のポインタ
 * phname: プレイスホルダ名
 * val_array: 置換文字列配列
 * column_size: 列サイズ
 * row_size: 行数
 *
 * 戻り値
 *  置換できた場合はゼロを返します。
 *  プレイスホルダ名が見つからないか%repeatコマンドが存在しない場合は -1 を返します。
 */
APIEXPORT int tpl_set_array(struct template_t* tpl,
                            const char* phname,
                            const char* val_array,
                            int column_size,
                            int row_size)
{
    struct tpl_array_t* v;
    struct tpl_array_t* last;
    char* src;
    int i;
    int vlen;
    int size;
    char* tp;

    if (! tpl->repeat_flag) {
        /* %repeatコマンドが存在しないため設定しません。*/
        return -1;
    }
    if (strlen(phname) > MAX_PHNAME) {
        err_write("template: place holder name too long: %s", phname);
        return -1;
    }

    /* プレイスホルダ名が存在するか調べます。*/
    if (find_place_holder(tpl, phname) == NULL)
        return -1;  /* not found */

    /* 配列の文字列長を算出します。*/
    vlen = 0;
    src = (char*)val_array;
    for (i = 0; i < row_size; i++) {
        vlen += strlen(src) + 1;
        src += column_size;
    }

    /* メモリ領域を確保します。*/
    size = sizeof(struct tpl_array_t) + row_size * sizeof(char*) + vlen;

    v = (struct tpl_array_t*)malloc(size);
    if (v == NULL) {
        err_write("template: no memory, tpl_set_array()");
        return -1;
    }
    v->val_array = (char**)((char*)v + sizeof(struct tpl_array_t));
    tp = (char*)v->val_array + row_size * sizeof(char*);
    src = (char*)val_array;

    for (i = 0; i < row_size; i++) {
        v->val_array[i] = tp;
        strcpy(v->val_array[i], src);
        tp += strlen(src) + 1;
        src += column_size;
    }
    strcpy(v->name, phname);
    v->array_size = row_size;
    v->next = NULL;

    /* リンクリストの最後に追加します。*/
    last = last_node_array(tpl);
    if (last == NULL)
        tpl->array_list = v;
    else
        last->next = v;
    return 0;
}

/*
 * テンプレートの eraseプレイスホルダに値を設定します。
 * 実際の置換処理は tpl_render() が呼び出された時点で行われます。
 *
 * tpl: テンプレート構造体のポインタ
 * phname: プレイスホルダ名
 * value: 消去する場合はゼロ以外の値
 *
 * 戻り値
 *  設定できた場合はゼロを返します。
 *  プレイスホルダ名が見つからないか%eraseコマンドが存在しない場合は -1 を返します。
 */
APIEXPORT int tpl_set_erase(struct template_t* tpl,
                            const char* phname,
                            int value)
{
    struct tpl_erase_t* v;
    struct tpl_erase_t* last;

    if (! tpl->erase_flag) {
        /* %eraseコマンドが存在しないため設定しません。*/
        return -1;
    }
    if (strlen(phname) > MAX_PHNAME) {
        err_write("template: place holder name too long: %s", phname);
        return -1;
    }

    /* プレイスホルダ名が存在するか調べます。*/
    if (find_place_holder(tpl, phname) == NULL)
        return -1;  /* not found */

    v = (struct tpl_erase_t*)malloc(sizeof(struct tpl_erase_t));
    if (v == NULL) {
        err_write("template: no memory, tpl_set_erase()");
        return -1;
    }

    strcpy(v->name, phname);
    v->value = value;
    v->next = NULL;

    /* リンクリストの最後に追加します。*/
    last = last_node_erase(tpl);
    if (last == NULL)
        tpl->erase_list = v;
    else
        last->next = v;
    return 0;
}

static int do_place_holder(struct template_t* tpl)
{
    struct tpl_object_t* o;
    int repc = 0;

    o = tpl->obj_list;
    while (o) {
        /* プレイスホルダが指定されているか？ */
        if (o->attr & ATTR_PLACEHOLDER) {
            struct tpl_value_t* v;

            v = tpl->value_list;
            while (v) {
                char ph[MAX_PHNAME];
                int n;

                /* プレイスホルダ名 */
                place_holder(v->name, ph, sizeof(ph));

                /* 該当のプレイスホルダ名が存在するか調べます。*/
                n = strstrc(o->value, ph);
                if (n > 0) {
                    int msize;

                    msize = (int)(strlen(v->value) - strlen(ph)) * n;
                    if (msize > 0) {
                        int new_size;
                        char* dst;

                        new_size = (int)strlen(o->value) + msize + 1;
                        dst = (char*)malloc(new_size);
                        if (dst == NULL) {
                            err_write("template: no memory, tpl_render()");
                            return -1;
                        }
                        strrep(o->value, ph, v->value, dst);
                        free(o->value);
                        o->value = dst;
                    } else {
                        char* dst;

                        dst = alloca(strlen(o->value) + 1);
                        strrep(o->value, ph, v->value, dst);
                        strcpy(o->value, dst);
                    }
                    repc++;
                }
                v = v->next;
            }
        }
        o = o->next;
    }
    return repc;
}

static int repeat_rows(struct template_t* tpl, struct tpl_object_t* start, struct tpl_object_t* end)
{
    struct tpl_object_t* o;
    int rows = 0;

    o = start;
    while (o) {
        struct tpl_array_t* v;

        v = tpl->array_list;
        while (v) {
            char ph[MAX_PHNAME];

            /* プレイスホルダ名 */
            place_holder(v->name, ph, sizeof(ph));

            /* 該当のプレイスホルダ名が存在するか調べます。*/
            if (strstr(o->value, ph)) {
                /* 配列要素数をチェックして最大値を設定します。*/
                if (v->array_size > rows)
                    rows = v->array_size;
            }
            v = v->next;
        }
        if (o == end)
            break;
        o = o->next;
    }
    return rows;
}

/* 複数のプレイスホルダを配列のインデックスの値で置換します。*/
static char* place_holder_array(struct template_t* tpl, struct tpl_object_t* o, int index)
{
    struct tpl_array_t* v;
    char* src;
    char* dst;
    int rep_flag = 0;

    src = (char*)alloca(strlen(o->value)+1);
    strcpy(src, o->value);

    v = tpl->array_list;
    while (v) {
        char ph[MAX_PHNAME];

        /* プレイスホルダ名 */
        place_holder(v->name, ph, sizeof(ph));

        /* 該当のプレイスホルダ名が存在するか調べます。*/
        if (strstr(src, ph)) {
            int cur_len;

            cur_len = (int)strlen(src);
            /* 配列の値を取得します。*/
            if (index < v->array_size) {
                int n;
                char* aval;
                int new_len;

                n = strstrc(src, ph);
                aval = v->val_array[index];
                new_len = (int)(cur_len - (strlen(ph) * n) + (strlen(aval) * n));
                dst = (char*)alloca(new_len+1);
                strrep(src, ph, aval, dst);
            } else {
                /* 配列外なので元の大きさを確保して空値に置換します。*/
                dst = (char*)alloca(cur_len+1);
                strrep(src, ph, "", dst);
            }
            src = dst;
            rep_flag = 1;
        }
        v = v->next;
    }

    if (rep_flag) {
        char* value;

        value = (char*)malloc(strlen(src)+1);
        if (value == NULL) {
            err_write("template: no memory, tpl_render()");
            return NULL;
        }
        strcpy(value, src);
        return value;
    }
    return NULL;
}

/* 処理した最後のノードポインタ(%repeat-end)が返されます。*/
static struct tpl_object_t* repeat(struct template_t* tpl, struct tpl_object_t* start)
{
    struct tpl_object_t* o;
    struct tpl_object_t* po;
    struct tpl_object_t* rs;
    struct tpl_object_t* re = NULL;
    struct tpl_object_t* last = NULL;
    int repc;
    int i;

    /* %repeat-endまでを抽出します。
     *
     * start-> <!--%repeat-->
     * rs ->   aaaaa
     *         bbbbb
     * re ->   ccccc
     *         <!--%repeat-end-->   <-このノードのポインタを返します。
     *         ...
     */
    o = start->next;  /* next %repeat stmt */
    po = start;
    rs = o;
    while (o) {
        if (o->attr & ATTR_CMD_REPEAT_END) {
            if (rs == o) {
                /* リピートする内容がない場合
                 *       <!--%repeat-->
                 *  rs-> <!--%repeat-end-->
                 */
                return o;
            }
            re = po;
            break;
        } else if (o->attr & ATTR_CMD_REPEAT) {
            /* 入れ子の場合は再帰的に処理します。*/
            o = repeat(tpl, o);
            if (o == NULL)
                return NULL;
        }
        po = o;
        o = o->next;
    }
    last = o;

    if (re == NULL) {
        /* %repeat-end が見つからない */
        err_write("template: not found %repeat-end: %s", tpl->file_name);
        return NULL;
    }

    /* 配列からプレイスホルダを置換する行数を求めます。*/
    repc = repeat_rows(tpl, rs, re);
    if (repc < 1) {
        /* 繰り返しなしため rs から re までのノードを削除します。*/
        delete_object_node(rs, re);
        start->next = last;
        return last;  /* 繰り返しなし */
    }

    /* rs から re までを rows分繰り返して生成します。
     *
     * <!-- %repeat -->
     * <tr>
     *   <td>@pcd</td>
     *   <td>@qty/@unit</td>
     *   <td>@price</td>
     * </tr>
     * <!-- %repeat-end -->
     */
    po = start;
    for (i = 0; i < repc; i++) {
        o = rs;
        while (o) {
            struct tpl_object_t* new_o;

            if (o->attr & ATTR_PLACEHOLDER) {
                char* new_value;

                /* ノードを新たに作成します。*/
                new_o = (struct tpl_object_t*)malloc(sizeof(struct tpl_object_t));
                if (new_o == NULL) {
                    err_write("template: no memory, tpl_render()");
                    return NULL;
                }
                memcpy(new_o, o, sizeof(struct tpl_object_t));

                /* プレイスホルダを該当する値で置換します。
                   動的に確保された領域が返されます。*/
                new_value = place_holder_array(tpl, o, i);
                if (new_value == NULL) {
                    /* 元の文字列を設定します。*/
                    new_o->value = (char*)malloc(strlen(o->value)+1);
                    if (new_o->value == NULL) {
                        err_write("template: no memory, tpl_render()");
                        return NULL;
                    }
                    strcpy(new_o->value, o->value);
                } else {
                    /* 置換された文字列を設定します。*/
                    new_o->value = new_value;
                }
            } else {
                /* 元のノードのクローンを作成します。*/
                int osize = (int)(sizeof(struct tpl_object_t) + strlen(o->value) + 1);
                new_o = (struct tpl_object_t*)malloc(osize);
                if (new_o == NULL) {
                    err_write("template: no memory, tpl_render()");
                    return NULL;
                }
                memcpy(new_o, o, sizeof(struct tpl_object_t));
                new_o->value = (char*)new_o + sizeof(struct tpl_object_t);
                strcpy(new_o->value, o->value);
            }

            po->next = new_o;
            po = new_o;
            if (o == re)
                break;
            o = o->next;
        }
    }

    /* rs から re までのノードを削除します。*/
    delete_object_node(rs, re);

    return last;
}

static int do_repeat(struct template_t* tpl)
{
    struct tpl_object_t* o;

    o = tpl->obj_list;
    while (o) {
        if (o->attr & ATTR_CMD_REPEAT) {
            /* 繰り返し処理を行ないます。
               処理した %repeat-end のポインタが返されます。*/
            o = repeat(tpl, o);
        }
        if (o)
            o = o->next;
    }
    return 0;
}

static int replace_word(const char* str, char* target, char* rep)
{
    char* tp;
    int index;

    *target = '\0';
    *rep = '\0';

    /* <!-- %replace "TARGET" "REPLACE" --> */
    tp = strstr(str, "replace");
    if (tp == NULL)
        return -1;

    index = indexof(tp, ' ');
    if (index >= 0) {
        tp += index + 1;
        index = indexof(tp, ' ');
        if (index >= 0) {
            substr(target, tp, 0, index);
            tp += index + 1;
            index = indexof(tp, '-');
            if (index >= 0) {
                substr(rep, tp, 0, index);
                /* 両端のスペースと引用符を取り除きます。*/
                trim(target);
                quote(target);
                trim(rep);
                quote(rep);
                return 0;
            }
        }
    }
    return -1;
}

/* 対象のオブジェクトを指しているオブジェクトを返します。*/
static struct tpl_object_t* prior_object(struct template_t* tpl, struct tpl_object_t* tp)
{
    struct tpl_object_t* o;

    o = tpl->obj_list;
    while (o) {
        if (o->next == tp) {
            return o;
        }
        o = o->next;
    }
    return NULL;
}

static int do_replace(struct template_t* tpl)
{
    struct tpl_object_t* o;

    o = tpl->obj_list;
    while (o) {
        if (o->attr & ATTR_CMD_REPLACE) {
            struct tpl_object_t* o2;
            char search_str[R_BUF_SIZE];
            char rep_str[R_BUF_SIZE];
            int search_len;
            int rep_len;

            /* 検索文字列と置換文字列を抽出します。*/
            replace_word(o->value, search_str, rep_str);

            search_len = (int)strlen(search_str);
            rep_len = (int)strlen(rep_str);

            /* 現在行以降に対して置換処理を行ないます。*/
            o2 = o->next;
            while (o2) {
                if (! (o2->attr & ATTR_COMMAND_ALL)) {
                    if (strstr(o2->value, search_str)) {
                        /* 置換対象が見つかった */
                        if (rep_len > search_len) {
                            int n;
                            int inc_size;

                            /* 置換対象の個数を求めます。*/
                            n = strstrc(o2->value, search_str);
                            /* 置換後の領域を拡張します。*/
                            inc_size = (rep_len - search_len) * n;

                            if (o2->attr & ATTR_PLACEHOLDER) {
                                char* tp;

                                tp = (char*)malloc(strlen(o2->value) + inc_size + 1);
                                if (tp == NULL) {
                                    err_write("template: no memory, tpl_render()");
                                    return -1;
                                }
                                strrep(o2->value, search_str, rep_str, tp);
                                free(o2->value);
                                o2->value = tp;
                            } else {
                                char* dst;
                                char* tp;
                                int new_size;

                                /* 文字列の置換を行ないます。*/
                                dst = (char*)alloca(strlen(o2->value) + inc_size + 1);
                                strrep(o2->value, search_str, rep_str, dst);

                                /* ノードの大きさを拡張します。*/
                                new_size = (int)(sizeof(struct tpl_object_t) + strlen(o2->value) + inc_size + 1);
                                tp = (char*)realloc(o2, new_size);
                                if (tp == NULL) {
                                    err_write("template: no memory, tpl_render()");
                                    return -1;
                                }
                                if (o2 != (struct tpl_object_t*)tp) {
                                    struct tpl_object_t* po;

                                    /* o2 を指しているリンクポインタを tp に更新します。*/
                                    po = prior_object(tpl, o2);
                                    if (po != NULL)
                                        po->next = (struct tpl_object_t*)tp;
                                }
                                /* 新しいアドレスを設定します。*/
                                o2 = (struct tpl_object_t*)tp;
                                o2->value = tp + sizeof(struct tpl_object_t);
                                strcpy(o2->value, dst);
                            }
                        } else {
                            char* dst;

                            /* 置換後も元の領域に収まります。*/
                            dst = (char*)alloca(strlen(o2->value) + 1);
                            strrep(o2->value, search_str, rep_str, dst);
                            strcpy(o2->value, dst);
                        }
                    }
                }
                o2 = o2->next;
            }
        }
        o = o->next;
    }
    return 0;
}

static int get_erase_value(struct template_t* tpl, const char* str)
{
    struct tpl_erase_t* v;

    v = tpl->erase_list;
    while (v) {
        char ph[MAX_PHNAME];

        /* プレイスホルダ名 */
        place_holder(v->name, ph, sizeof(ph));

        /* 該当のプレイスホルダ名が存在するか調べます。*/
        if (strstr(str, ph))
            return v->value;
        v = v->next;
    }
    return -1;
}

/* erase-endまでのノードを削除して次のノードポインタを返します。*/
static struct tpl_object_t* erase_to_end(struct tpl_object_t* start)
{
    struct tpl_object_t* o;
    int erase = 0;

    o = start;
    do {
        struct tpl_object_t* next;

        if (o->attr & ATTR_CMD_ERASE_END)
            erase--;
        else if (o->attr & ATTR_CMD_ERASE)
            erase++;

        next = o->next;
        if (o->attr & ATTR_PLACEHOLDER) {
            if (o->value)
                free(o->value);
        }
        free(o);
        o = next;
    } while (o && erase > 0);
    return o;
}

static int do_erase(struct template_t* tpl)
{
    struct tpl_object_t* o;
    struct tpl_object_t* po;  /* prior node */

    o = tpl->obj_list;
    po = NULL;
    while (o) {
        if (o->attr & ATTR_CMD_ERASE) {
            int eval;

            eval = get_erase_value(tpl, o->value);
            if (eval < 0) {
                /* 該当しないため何もしない */
            } else if (eval == 0) {
                /* erase から erase-end までを残す。*/
            } else {
                /* erase から erase-end までを消す。*/
                o = erase_to_end(o);
                if (po == NULL)
                    tpl->obj_list = o;
                else
                    po->next = o;
                continue;
            }
        }
        po = o;
        o = o->next;
    }
    return 0;
}

/*
 * テンプレート処理を実行します。
 *
 * tpl: テンプレート構造体のポインタ
 *
 * 戻り値
 *  正常に処理できた場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
APIEXPORT int tpl_render(struct template_t* tpl)
{
    tpl->out_size = 0;

    /* プレイスホルダを置換します。*/
    if (do_place_holder(tpl) < 0)
        return -1;

    if (tpl->repeat_flag) {
        /* repeatコマンドの実行 */
        if (do_repeat(tpl) < 0)
            return -1;
    }

    if (tpl->replace_flag) {
        /* replaceコマンドの実行 */
        if (do_replace(tpl) < 0)
            return -1;
    }

    if (tpl->erase_flag) {
        /* eraseコマンドの実行 */
        if (do_erase(tpl) < 0)
            return -1;
    }
    return 0;
}

/* 処理結果を文字列として生成します。
   文字列を格納するメモリは自動的に確保されます。*/
static char* tpl_xstrcat(struct template_t* tpl, char* s)
{
    int len;

    if (tpl->out_alloc_size == 0) {
        int alloc_size;

        /* メモリ領域をファイルサイズの2倍確保します。*/
        alloc_size = tpl->file_size * 2;
        tpl->out_data = (char*)malloc(alloc_size);
        if (tpl->out_data == NULL) {
            err_write("template: no memory, tpl_xstrcat()");
            return NULL;
        }
        tpl->out_alloc_size = alloc_size;
        tpl->out_size = 0;
        *tpl->out_data = '\0';
    }

    len = (int)strlen(s);
    if (tpl->out_size + len + 1 > tpl->out_alloc_size) {
        char* t;
        int alloc_size;

        /* メモリ領域を増分します。*/
        alloc_size = tpl->out_alloc_size + tpl->file_size;
        t = (char*)realloc(tpl->out_data, alloc_size);
        if (t == NULL) {
            err_write("template: no memory, tpl_xstrcat()");
            return NULL;
        }
        tpl->out_data = t;
        tpl->out_alloc_size = alloc_size;
    }
    strcat(tpl->out_data, s);
    tpl->out_size += len;
    return tpl->out_data;
}

/*
 * テンプレート処理が行われたデータを取得します。
 * データの最後に文字列の終端を示す'\0'が付加されます。
 * この関数で返されたデータのポインタは tpl_close() を行なうと解放されます。
 *
 * 出力サイズにデータの最後の'\0'は含まれません。
 *
 * 出力エンコーディング名が指定されている場合にテンプレートファイルの
 * エンコーディングと違う場合に文字コード変換が行なわれます。
 * 出力エンコーディングがNULLかテンプレートファイルのエンコーディングが
 * NULLの場合には文字コード変換は行なわれません。
 *
 * tpl: テンプレート構造体のポインタ
 * out_encoding: 出力するエンコーディング名
 * data_size: 出力長が設定されるポインタ
 *
 * 戻り値
 *  データのポインタを返します。
 *  エラーの場合は NULL を返します。
 */
APIEXPORT char* tpl_get_data(struct template_t* tpl,
                             const char* out_encoding,
                             int* data_size)
{
    struct tpl_object_t* o;

    if (tpl->out_size > 0) {
        /* 作成済み */
        *data_size = tpl->out_size;
        return tpl->out_data;
    }

    /* リストから文字列を作成します。*/
    o = tpl->obj_list;
    while (o) {
        /* コマンド行は対象外とします。*/
        if (! (o->attr & ATTR_COMMAND_ALL)) {
            if (tpl_xstrcat(tpl, o->value) == NULL)
                return NULL;
        }
        o = o->next;
    }

    if (strlen(tpl->file_enc) > 0 && out_encoding != NULL) {
        if (stricmp(tpl->file_enc, out_encoding)) {
            int osize;
            char* buff;
            int buffsize;

            /* 文字コード変換を行ないます。*/
            buffsize = tpl->out_size * 2;
            buff = (char*)malloc(buffsize);
            if (buff == NULL) {
                err_write("template: no memory, tpl_get_data()");
                return NULL;
            }
            osize = convert(tpl->file_enc, tpl->out_data, tpl->out_size, out_encoding, buff, buffsize);
            if (osize < 0) {
                free(buff);
                err_write("template: iconv error: %s(%s) to %s", tpl->file_name, tpl->file_enc, out_encoding);
                return NULL;
            }
            /* 変換後のバッファと入れ替えます。*/
            free(tpl->out_data);
            tpl->out_data = buff;
            tpl->out_size = osize;
            tpl->out_alloc_size = buffsize;
        }
    }
    *data_size = tpl->out_size;
    return tpl->out_data;
}

/*
 * テンプレートをクローズします。
 * 確保されたメモリが開放されます。
 *
 * tpl: テンプレート構造体のポインタ
 *
 * 戻り値
 *  なし
 */
APIEXPORT void tpl_close(struct template_t* tpl)
{
    if (tpl) {
        if (tpl->obj_list)
            delete_object_node(tpl->obj_list, NULL);

        if (tpl->value_list)
            delete_value_node(tpl->value_list);

        if (tpl->array_list)
            delete_array_node(tpl->array_list);

        if (tpl->erase_list)
            delete_erase_node(tpl->erase_list);

        if (tpl->out_data)
            free(tpl->out_data);

        /* クリティカルセクションの削除 */
        CS_DELETE(&tpl->critical_section);

        free(tpl);
    }
}
