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
#ifndef _SMTP_H_
#define _SMTP_H_

#include "apiexp.h"
#include "nestalib.h"

/* SMTP session struct */
struct smtp_session_t {
    char server[512];
    SOCKET c_socket;
    int open_session_flag;
    char* date;
    char* subject;
    char* to;
    char* from;
    char* cc;
    char* bcc;
    struct hash_t* header;
    char* msg;
    struct vector_t* attach_vt;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct smtp_session_t* smtp_open(const char* server, ushort port);
APIEXPORT void smtp_close(struct smtp_session_t* smtp);
APIEXPORT void smtp_set_date(struct smtp_session_t* smtp, const char* date);
APIEXPORT void smtp_set_subject(struct smtp_session_t* smtp, const char* subject);
APIEXPORT void smtp_set_to(struct smtp_session_t* smtp, const char* to);
APIEXPORT void smtp_set_from(struct smtp_session_t* smtp, const char* from);
APIEXPORT void smtp_set_cc(struct smtp_session_t* smtp, const char* cc);
APIEXPORT void smtp_set_bcc(struct smtp_session_t* smtp, const char* bcc);
APIEXPORT void smtp_set_header(struct smtp_session_t* smtp,
                               const char* name, const char* value);
APIEXPORT void smtp_set_body(struct smtp_session_t* smtp, const char* msg);
APIEXPORT void* smtp_add_attach(struct smtp_session_t* smtp,
                                const char* content_type,
                                const char* transfer_encoding,
                                const char* file_name,
                                const char* enc_data);
APIEXPORT void smtp_delete_attach(struct smtp_session_t* smtp, const void* afid);
APIEXPORT int smtp_send(struct smtp_session_t* smtp);

#ifdef __cplusplus
}
#endif

#endif /* _SMTP_H_ */
