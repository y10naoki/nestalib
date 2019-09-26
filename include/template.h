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
#ifndef _TEMPLATE_H_
#define _TEMPLATE_H_

#include "apiexp.h"
#include "nestalib.h"

#define MAX_PHNAME 256

/* node attr */
#define ATTR_CMD_ERASE       0x0001  /* erase command */
#define ATTR_CMD_ERASE_END   0x0002  /* erase-end command */
#define ATTR_CMD_REPEAT      0x0004  /* repeat command */
#define ATTR_CMD_REPEAT_END  0x0008  /* repeat-end command */
#define ATTR_CMD_REPLACE     0x0010  /* replace command */
#define ATTR_PLACEHOLDER     0x1000  /* has place holder */

#define ATTR_COMMAND     (ATTR_CMD_ERASE|ATTR_CMD_REPEAT|ATTR_CMD_REPLACE)
#define ATTR_COMMAND_ALL (ATTR_COMMAND|ATTR_CMD_ERASE_END|ATTR_CMD_REPEAT_END)

/* source node */
struct tpl_object_t {
    unsigned int attr;
    char* value;
    struct tpl_object_t* next;
};

/* value node */
struct tpl_value_t {
    char name[MAX_PHNAME];
    char* value;
    struct tpl_value_t* next;
};

/* array node */
struct tpl_array_t {
    char name[MAX_PHNAME];
    char** val_array;
    int array_size;
    struct tpl_array_t* next;
};

/* erase node */
struct tpl_erase_t {
    char name[MAX_PHNAME];
    int value;
    struct tpl_erase_t* next;
};

/* template struct */
struct template_t {
    CS_DEF(critical_section);  /* use in tpl_reopen() */
    char dir_name[MAX_PATH];
    char file_name[MAX_PATH];
    struct stat file_stat;
    char file_enc[64];              /* template file encoding */
    struct tpl_object_t* obj_list;  /* source node list */
    struct tpl_object_t* last_obj;  /* last of source node */
    int file_size;                  /* template file size */
    int replace_flag;               /* has %replace command? */
    int repeat_flag;                /* has %repeat command? */
    int erase_flag;                 /* has %erase command? */
    struct tpl_value_t* value_list; /* value node list */
    struct tpl_array_t* array_list; /* array node list */
    struct tpl_erase_t* erase_list; /* erase node list */
    int out_alloc_size;             /* alloc size */
    int out_size;                   /* real size */
    char* out_data;                 /* output data */
};

#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct template_t* tpl_open(const char* dir_name, const char* file_name, const char* encoding);
APIEXPORT struct template_t* tpl_reopen(struct template_t* tpl);
APIEXPORT int tpl_set_value(struct template_t* tpl, const char* phname, const char* value);
APIEXPORT int tpl_set_array(struct template_t* tpl, const char* phname, const char* val_array, int column_size, int row_size);
APIEXPORT int tpl_set_erase(struct template_t* tpl, const char* phname, int value);
APIEXPORT int tpl_render(struct template_t* tpl);
APIEXPORT char* tpl_get_data(struct template_t* tpl, const char* out_encoding, int* data_size);
APIEXPORT void tpl_close(struct template_t* tpl);

#ifdef __cplusplus
}
#endif

#endif /* _TEMPLATE_H_ */
