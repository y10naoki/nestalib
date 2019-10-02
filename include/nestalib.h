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
#ifndef _NESTALIB_H_
#define _NESTALIB_H_

#ifndef MAC_OSX
#if defined(__APPLE__) && defined(__MACH__)
#define MAC_OSX
#endif
#endif

#ifdef _MSC_VER
#  if _MSC_VER >= 1400 /* Visual Studio 2005 and up */
#    pragma warning(disable:4996) // unsecure
#  endif
#endif

#ifdef _WIN32
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif

#ifndef _WIN32
#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64
#endif

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <errno.h>
#include <stdio.h>
#include <io.h>
#include <fcntl.h>
#include <process.h>
#include <signal.h>
#include <sys/stat.h>
#else
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <limits.h>
#endif

#ifdef MAC_OSX
#include <sys/_select.h>
#endif

#ifndef _WIN32
#define MAX_PATH  PATH_MAX
#define INVALID_SOCKET  (-1)
#endif

#ifndef __USE_MISC
#ifndef ushort
typedef unsigned short  ushort;
#endif
#ifndef uint
typedef unsigned int    uint;
#endif
#ifndef ulong
typedef unsigned long   ulong;
#endif
#endif

#ifndef uchar
typedef unsigned char   uchar;
#endif
#ifndef int32
typedef int int32;
#endif
#ifndef int64
typedef long long int64;
#endif
#ifndef uint64
typedef unsigned long long   uint64;
#endif

#ifndef _WIN32
#ifndef SOCKET
typedef int SOCKET;
#endif
#endif

#define MAX_ZONENAME    31

#ifdef _WIN32
#define FILE_OPEN _open
#define FILE_CLOSE _close
#define FILE_READ _read
#define FILE_WRITE _write
#define FILE_SEEK _lseeki64
#define FILE_TRUNCATE _chsize_s
#define SOCKET_CLOSE closesocket
#define snprintf _snprintf
#define alloca _alloca
#define sleep Sleep
#define atoi64 _atoi64
#define getpid _getpid
#else
#define FILE_OPEN open
#define FILE_CLOSE close
#define FILE_READ read
#define FILE_WRITE safe_write
#define FILE_SEEK lseek
#define FILE_TRUNCATE ftruncate
#define SOCKET_CLOSE close
#define stricmp strcasecmp
#define strnicmp strncasecmp
#define atoi64 atoll
#endif

#ifdef _WIN32
#define CREATE_MODE     S_IREAD|S_IWRITE
#define S_ISDIR(m)      (m & _S_IFDIR)
#else
#define O_BINARY        0x0
#define CREATE_MODE     S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH
#endif

#define SERVER_NAME     "nesta/1.1.1b"

#define BUF_SIZE        2048    /* send, recv buffer size */

/* application zone */
struct appzone_t {
    char zone_name[MAX_ZONENAME];         /* zone name */
    int max_session;                      /* max session number */
    int session_timeout;                  /* session timeout */
    struct zone_session_t* zone_session;  /* zone session */
};

#define RCV_TIMEOUT_NOWAIT  0
#define RCV_TIMEOUT_WAIT    -1

/* request info. */
#define MAX_URI_LENGTH      2047
#define MAX_CONTENT_NAME    255
#define MAX_REQ_HEADER      64
#define MAX_REQ_VARIABLE    128

#define MAX_RECV_DATA_SIZE      (1024*1024)    /* 1MB */
#define MAX_METHOD_LINE_SIZE    (MAX_URI_LENGTH+32)

#define MAX_VNAME_SIZE  64      /* variable name max size */
#define MAX_VVALUE_SIZE 2000    /* variable value max size */

struct variable_t {
    char* name;                 /* name is NULL terminated */
    char* value;                /* value is NULL terminated
                                   but attach file data
                                   is not NULL terminated */
};

struct attach_file_t {
    char filename[MAX_PATH];             /* attach file name */
    char mimetype[MAX_VNAME_SIZE+1];     /* MIME type */
    char charset[MAX_VNAME_SIZE+1];      /* charset */
    unsigned char* data;                 /* data pointer(same value in struct_variable_t) */
    int size;                            /* data size */
};

struct http_header_t {
    int count;
    struct variable_t vt[MAX_REQ_HEADER];
};

struct query_param_t {
    int count;
    struct variable_t vt[MAX_REQ_VARIABLE];
    struct attach_file_t* af[MAX_REQ_VARIABLE];
};

struct request_t {
    char method[8];                         /* GET, POST or HEAD */
    char uri[MAX_URI_LENGTH+1];             /* uri string */
    char protocol[16];                      /* HTTP/1.1 */
    struct in_addr addr;                    /* client IP addr */
    int qs_index;                           /* '?'char index, notfound is -1 */
    char content_name[MAX_CONTENT_NAME+1];  /* document or web/service name(cut starting '/') */
    struct http_header_t header;            /* header parameters */
    struct query_param_t q_param;           /* query parameters */
    struct vector_t* heap;                  /* request heap memory */
    struct zone_session_t* zone;            /* zone session */
    struct session_t* session;              /* session info */
    int64 start_time;                       /* start time(usec) */
};

struct response_t {
    SOCKET socket;                          /* output socket */
    int content_size;                       /* content-length */
};

/* HTTP Status */
#define HTTP_OK                     200
#define HTTP_NOT_MODIFIED           304
#define HTTP_BADREQUEST             400
#define HTTP_NOTFOUND               404
#define HTTP_REQUEST_TIMEOUT        408
#define HTTP_REQUEST_URI_TOO_LONG   414
#define HTTP_INTERNAL_SERVER_ERROR  500
#define HTTP_NOTIMPLEMENT           501

/* user parameters define */
#define MAX_USER_VARIABLE   100

struct user_param_t {
    int count;
    struct variable_t vt[MAX_USER_VARIABLE];
};

/* content API function
 *
 * return value is http status
 */
typedef int (*API_FUNCPTR)(struct request_t* req,
                           struct response_t* resp,
                           struct user_param_t* uparam);

/* request hook API */
struct hook_api_t {
    char content_name[MAX_CONTENT_NAME+1];
    struct appzone_t* app_zone;
    API_FUNCPTR func_ptr;
};

/* init/term API function */
typedef int (*HOOK_FUNCPTR)(struct user_param_t* uparam);

/* socket event callback function */
typedef int (*SOCK_EVENT_CB)(SOCKET socket);
/* socket event loop function(true is loop break) */
typedef int (*SOCK_EVENT_BREAK_CB)(void);

/* socket buffer */
struct sock_buf_t {
    SOCKET socket;
    int bufsize;
    char* buf;
    int cur_size;
};

#include "apiexp.h"
#include "csect.h"
#include "mtfunc.h"
#include "cgiutils.h"
#include "strutil.h"
#include "fcache.h"
#include "queue.h"
#include "hash.h"
#include "pool.h"
#include "session.h"
#include "template.h"
#include "vector.h"
#include "zlibutil.h"
#include "smtp.h"
#include "md5.h"
#include "syscall.h"
#include "mmap.h"
#include "btree.h"
#include "dataio.h"
#include "nio.h"
#include "memutil.h"

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* recv.c */
APIEXPORT int wait_recv_data(SOCKET socket, int timeout_ms);
APIEXPORT char* recv_data(SOCKET socket, int check_size, int timeout_ms, void* ssl, int* recv_size);
APIEXPORT void recv_free(const char* ptr);
APIEXPORT int recv_char(SOCKET socket, char* buf, int bufsize, int* status);
APIEXPORT int recv_nchar(SOCKET socket, char* buf, int bytes, int* status);
APIEXPORT short recv_short(SOCKET socket, int* status);
APIEXPORT int recv_int(SOCKET socket, int* status);
APIEXPORT int64 recv_int64(SOCKET socket, int* status);
APIEXPORT int recv_line(SOCKET socket, char* buf, int bufsize, const char* delimiter);
APIEXPORT char* recv_str(SOCKET socket, const char* delimiter, int delim_add_flag);

/* send.c */
APIEXPORT int send_data(SOCKET socket, const void* buf, int buf_size);
APIEXPORT int send_short(SOCKET socket, short data);
APIEXPORT int send_int64(SOCKET socket, int64 data);

/* request.c */
APIEXPORT struct request_t* get_request(SOCKET socket, struct in_addr addr, int* status);
APIEXPORT void req_free(struct request_t* req);

/* response.c */
APIEXPORT struct response_t* resp_initialize(SOCKET socket);
APIEXPORT void resp_finalize(struct response_t* resp);
APIEXPORT int resp_send_header(struct response_t* resp, struct http_header_t* hdr);
APIEXPORT int resp_send_body(struct response_t* resp, const void* body, int body_size);
APIEXPORT int resp_send_data(struct response_t* resp, const void* data, int data_size);
APIEXPORT void resp_set_content_size(struct response_t* resp, int content_size);

/* req_heap.c */
APIEXPORT void* xalloc(struct request_t* req, int size);
APIEXPORT void* xrealloc(struct request_t* req, const void* ptr, int resize);
APIEXPORT void xfree(struct request_t* req, const void* ptr);

/* query.c */
APIEXPORT char* get_qparam(struct request_t* req, const char* name);
APIEXPORT int get_qparam_count(struct request_t* req);
APIEXPORT struct attach_file_t* get_attach_file(struct request_t* req, const char* name);

/* handler.c */
APIEXPORT int head_handler(SOCKET socket, int* content_size);
APIEXPORT int forward_handler(SOCKET socket, int http_status, int* content_size);
APIEXPORT int error_handler(SOCKET socket, int http_status, int* content_size);

/* sock.c */
APIEXPORT void sock_initialize(void);
APIEXPORT void sock_finalize(void);
APIEXPORT int sock_mkserver(const char* host, ushort port, struct sockaddr_in* server);
APIEXPORT SOCKET sock_connect_server(const char* host, ushort port);
APIEXPORT char* sock_local_addr(char* ip_addr);
APIEXPORT SOCKET sock_listen(ulong addr, ushort port, int backlog, struct sockaddr_in* sockaddr);

/* url.c */
APIEXPORT char* url_post(const char* url, struct http_header_t* header, const char* query, const char* proxy_server, ushort proxy_port, int* res_size);
APIEXPORT int url_http_status(const char* msg);

/* user_param.c */
APIEXPORT char* get_user_param(struct user_param_t* u_param, const char* name);

/* header.c */
APIEXPORT struct http_header_t* alloc_http_header(void);
APIEXPORT void init_http_header(struct http_header_t* hdr);
APIEXPORT void free_http_header(struct http_header_t* hdr);
APIEXPORT int set_http_header(struct http_header_t* hdr, const char* name, const char* value);
APIEXPORT int set_content_type(struct http_header_t* hdr, const char* type, const char* charset);
APIEXPORT int set_content_length(struct http_header_t* hdr, int length);
APIEXPORT int set_cookie(struct http_header_t* hdr, const char* name, const char* cvalue, const char* expires, long maxage, const char* domain, const char* path, int secure);
APIEXPORT int set_http_session(struct http_header_t* hdr, struct session_t* s);
APIEXPORT char* get_http_header(struct http_header_t* hdr, const char* name);
APIEXPORT int split_item(char* str, struct variable_t* vt, char delim);
APIEXPORT void free_item(struct variable_t* vt);
APIEXPORT char* split_header(char* msg, struct http_header_t* hdr);
APIEXPORT int get_header_length(const char* msg);
APIEXPORT char* get_cookie(struct http_header_t* hdr, const char* cname, char* cvalue);
APIEXPORT struct session_t* get_http_session(struct zone_session_t* zs, struct http_header_t* hdr);
APIEXPORT int send_header(SOCKET socket, struct http_header_t* hdr);

/* datetime.c */
APIEXPORT char* todays(char* buf, int buf_size, char* sep);
APIEXPORT char* gmtstr(char* buf, int buf_size, struct tm* gmtime);
APIEXPORT char* now_gmtstr(char* buf, int buf_size);
APIEXPORT char* jststr(char* buf, int buf_size, struct tm* jstime);
APIEXPORT char* now_jststr(char* buf, int buf_size);
APIEXPORT int64 system_time(void);
APIEXPORT uint system_seconds(void);

/* error.c */
APIEXPORT void err_initialize(const char* error_file);
APIEXPORT void err_finalize(void);
APIEXPORT void err_write(const char* fmt, ...);
APIEXPORT void err_log(struct in_addr addr, const char* fmt, ...);

/* logout.c */
APIEXPORT void logout_initialize(const char* out_file);
APIEXPORT void logout_finalize(void);
APIEXPORT void logout_write(const char* fmt, ...);

/* srelay_client.c */
APIEXPORT struct srelay_server_t* srelay_initialize(int count, const char* host_tbl[], ushort port_tbl[], int check_interval_time, ulong host_addr, ushort host_port);
APIEXPORT void srelay_finalize(struct srelay_server_t* rsvr);
APIEXPORT int srelay_get_session(struct session_t* s, const char* skey, const char* zone, const char* host, ushort port, const char* owner_host, ushort owner_port, struct session_copy_t* owner_s_cp, struct session_copy_t* s_cp_failover);
APIEXPORT int64 srelay_timestamp(const char* skey, const char* zone, const char* host, ushort port, struct session_copy_t* s_cp_failover);
APIEXPORT int srelay_change_owner(struct session_t* s, const char* skey, const char* zone, const char* host, ushort port, const char* owner_host, ushort owner_port);
APIEXPORT int srelay_delete_session(const char* skey, const char* zone, const char* host, ushort port);
APIEXPORT int srelay_copy_session(struct session_t* s, const char* skey, const char* zone, const char* host, ushort port, const char* owner_host, ushort owner_port, struct session_copy_t* s_cp);

/* base64.c */
APIEXPORT char* base64_encode(char* dst, const void* src, int src_size);
APIEXPORT int base64_decode(void* dst, const char* src);

/* mime.c */
APIEXPORT char* mime_encode(char* dst, int dst_size, const char* src, const char* src_enc);
APIEXPORT char* mime_decode(char* dst, int dst_size, const char* dst_enc, const char* mime_str);

/* sockevent.c */
APIEXPORT void sock_event(int sc, const SOCKET sockets[], const SOCK_EVENT_CB cbfuncs[], const SOCK_EVENT_BREAK_CB breakfunc);
APIEXPORT void* sock_event_create(void);
APIEXPORT int sock_event_add(const void* sev, SOCKET socket);
APIEXPORT int sock_event_delete(const void* sev, SOCKET socket);
APIEXPORT int sock_event_disable(const void* sev, SOCKET socket);
APIEXPORT int sock_event_enable(const void* sev, SOCKET socket);
APIEXPORT void sock_event_loop(const void* sev, const SOCK_EVENT_CB cbfuncs, const SOCK_EVENT_BREAK_CB breakfunc);
APIEXPORT void sock_event_close(const void* sev);

/* sockbuf.c */
APIEXPORT struct sock_buf_t* sockbuf_alloc(SOCKET socket);
APIEXPORT void sockbuf_free(struct sock_buf_t* sb);
APIEXPORT int sockbuf_wait_data(struct sock_buf_t* sb, int timeout_ms);
APIEXPORT int sockbuf_read(struct sock_buf_t* sb, char* buf, int size);
APIEXPORT int sockbuf_nchar(struct sock_buf_t* sb, char* buf, int size);
APIEXPORT short sockbuf_short(struct sock_buf_t* sb, int* status);
APIEXPORT int sockbuf_int(struct sock_buf_t* sb, int* status);
APIEXPORT int64 sockbuf_int64(struct sock_buf_t* sb, int* status);
APIEXPORT int sockbuf_gets(struct sock_buf_t* sb, char* buf, int size, const char* delim, int delim_add_flag, int* found_flag);

#ifdef __cplusplus
}
#endif

#endif  /* _NESTALIB_H_ */
