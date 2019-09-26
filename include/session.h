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
#ifndef _SESSION_H_
#define _SESSION_H_

#include "nestalib.h"

#define MAX_SESSIONID       256
#define SESSION_KEY_SIZE    32
#define MAX_HOSTNAME        256

#define SESSION_UNLIMITED   (-1)
#define SESSION_NOTIMEOUT   (-1)

#define SESSIONID_NAME      "nxsessionid"

#define MAX_SESSION_RELAY_COPY  3

/* callback get ip and port */
typedef ulong (*CALLBACK_HOST)(ushort*);

/* callback get ip and port */
typedef int (*CALLBACK_SESSION_COPY)(ulong*, ushort*);

/* session copy struct */
struct session_copy_t {
    int count;
    ulong addr[MAX_SESSION_RELAY_COPY];
    ushort port[MAX_SESSION_RELAY_COPY];
};

/* session relay server struct */
struct srelay_server_t {
    ulong host_addr;            /* own ip */
    ushort host_port;           /* own port */
    struct session_copy_t s_cp; /* copy to server info */
    int check_interval_time;    /* seconds */
    struct hash_t* rs_tbl;      /* key:   host(ip-address) */
                                /* value: struct rserver_info_t* */
    int thread_end_flag;        /* server end flag */
};

/* session data struct */
struct session_data_t {
    int size;                   /* zero is no area */
    void* data;                 /* data pointer */
};

/* session struct */
struct session_t {
    struct zone_session_t* zs;      /* zone session */
    char sid[MAX_SESSIONID];        /* session id */
    char skey[SESSION_KEY_SIZE+1];  /* session key */
    int64 last_access;              /* last access(usec) */
    int owner_flag;                 /* session owner flag */
    ulong owner_addr;               /* current owner ip */
    ushort owner_port;              /* current owner port */
    struct session_copy_t owner_s_cp;  /* current owner copy server */
    int64 last_update;              /* last update(usec)*/
    struct hash_t* sdata;           /* key:   session name */
                                    /* value: session data struct */
    int64 attach_last_update;       /* last attach(usec) */
};

/* zone session struct */
struct zone_session_t {
    CS_DEF(critical_section);
    char zone_name[MAX_ZONENAME]; /* zone name */
    int max_session;              /* -1 is unlimited */
    int cur_session;              /* current session number */
    int timeout;                  /* -1 is no timeout */
    struct hash_t* s_tbl;         /* key:   session key */
                                  /* value: session struct pointer */
    struct srelay_server_t* rsvr; /* session relay server pointer */
    int thread_end_flag;          /* server end flag */
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

APIEXPORT struct zone_session_t* ssn_initialize(const char* zname, int max_session, int timeout, struct srelay_server_t* rs);
APIEXPORT void ssn_finalize(struct zone_session_t* zs);
APIEXPORT struct session_t* ssn_create(struct request_t* req);
APIEXPORT struct session_t* ssn_copy_create(struct zone_session_t* zs, const char* skey, const char* sid);
APIEXPORT struct session_t* ssn_target(struct zone_session_t* zs, const char* sid);
APIEXPORT void ssn_free_nolock(struct session_t* s);
APIEXPORT void ssn_close(struct zone_session_t* zs, const char* sid);
APIEXPORT void ssn_close_nolock(struct zone_session_t* zs, const char* sid);
APIEXPORT void* ssn_get(struct session_t* s, const char* key);
APIEXPORT void ssn_put(struct session_t* s, const char* key, const char* str);
APIEXPORT void ssn_putdata(struct session_t* s, const char* key, const void* data, int size);
APIEXPORT void ssn_put_nolock(struct session_t* s, const char* key, const void* data, int size);
APIEXPORT void ssn_delete(struct session_t* s, const char* key);
APIEXPORT void ssn_delete_all(struct session_t* s);
APIEXPORT void ssn_delete_all_nolock(struct session_t* s);
APIEXPORT void ssn_attach(struct session_t* s);
APIEXPORT void ssn_detach(struct session_t* s);

#ifdef __cplusplus
}
#endif

#endif /* _SESSION_H_ */
