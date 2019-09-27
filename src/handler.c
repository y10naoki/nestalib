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

#define API_INTERNAL
#include "nestalib.h"

/* HEAD request */
static char* head_template =
    "HTTP/1.1 200 OK\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Connection: close\r\n"
    "\r\n";

/* Not Modified */
static char* fwd_template_304 =
    "HTTP/1.1 304 Not Modified\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Connection: close\r\n"
    "\r\n";

/* bad request */
static char* err_template_400 =
    "HTTP/1.1 400 Bad Request\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n"
    "%s";

static char* err_html_400 =
    "<html>\n"
    "<head><title>400 Bad Request</title></head>"
    "<body>\n"
    "<h1>400 Bad Request</h1>\n"
    "<p>This server could not understand your request.</p>\n"
    "</body>\n"
    "</html>";

/* not found */
static char* err_template_404 =
    "HTTP/1.1 404 Not Found\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n"
    "%s";

static char* err_html_404 =
    "<html>\n"
    "<head><title>404 Not Found</title></head>"
    "<body>\n"
    "<h1>404 Not Found</h1>\n"
    "<p>No such file.</p>\n"
    "</body>\n"
    "</html>";

/* Request Timeout */
static char* err_template_408 =
    "HTTP/1.1 408 Request Timeout\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n"
    "%s";

static char* err_html_408 =
    "<html>\n"
    "<head><title>408 Request Timeout</title></head>"
    "<body>\n"
    "<h1>408 Request Timeout</h1>\n"
    "<p>Your request was timeout.</p>\n"
    "</body>\n"
    "</html>";

/* request-URI too long */
static char* err_template_414 =
    "HTTP/1.1 414 Request-URI Too Long\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n"
    "%s";

static char* err_html_414 =
    "<html>\n"
    "<head><title>414 Request-URI Too Long</title></head>"
    "<body>\n"
    "<h1>414 Request-URI Too Long</h1>\n"
    "<p>Request URI too long.</p>\n"
    "</body>\n"
    "</html>";

/* internal server error */
static char* err_template_500 =
    "HTTP/1.1 500 Internal Server Error\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n"
    "%s";

static char* err_html_500 =
    "<html>\n"
    "<head><title>500 Internal Server Error</title></head>"
    "<body>\n"
    "<h1>500 Internal Server Error</h1>\n"
    "<p>Internal Server Error.</p>\n"
    "</body>\n"
    "</html>";

/* method not implemented */
static char *err_template_501 =
    "HTTP/1.1 501 Method Not Implemented\r\n"
    "Date: %s\r\n"
    "Server: %s\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: %d\r\n"
    "Connection: close\r\n"
    "\r\n"
    "%s";

static char *err_html_501 =
    "<html>\n"
    "<head><title>501 Method Not Implemented</title></head>"
    "<body>\n"
    "<h1>501 Method Not Implemented</h1>\n"
    "<p>method not implemented.</p>\n"
    "</body>\n"
    "</html>";

APIEXPORT int head_handler(SOCKET socket, int* content_size)
{
    char now_date[256];
    char send_buff[BUF_SIZE];

    /* 現在時刻をGMTで取得 */
    now_gmtstr(now_date, sizeof(now_date));

    snprintf(send_buff, sizeof(send_buff), head_template, now_date, SERVER_NAME);
    send_data(socket, send_buff, (int)strlen(send_buff));
    *content_size = 0;
    return HTTP_OK;
}

APIEXPORT int forward_handler(SOCKET socket, int http_status, int* content_size)
{
    char now_date[256];
    char send_buff[BUF_SIZE];

    /* 現在時刻をGMTで取得 */
    now_gmtstr(now_date, sizeof(now_date));

    switch (http_status) {
        case HTTP_NOT_MODIFIED:
            *content_size = 0;
            snprintf(send_buff, sizeof(send_buff), fwd_template_304,
                     now_date, SERVER_NAME);
            break;

        default:    /* HTTP_INTERNAL_SERVER_ERROR */
            *content_size = (int)strlen(err_html_500);
            snprintf(send_buff, sizeof(send_buff), err_template_500,
                     now_date, SERVER_NAME, *content_size, err_html_500);
            break;
    }

    send_data(socket, send_buff, (int)strlen(send_buff));
    return http_status;
}

APIEXPORT int error_handler(SOCKET socket, int http_status, int* content_size)
{
    char* temp;
    char* html;
    char now_date[256];
    char send_buff[BUF_SIZE];

    switch (http_status) {
        case HTTP_BADREQUEST:
            temp = err_template_400;
            html = err_html_400;
            break;

        case HTTP_NOTFOUND:
            temp = err_template_404;
            html = err_html_404;
            break;

        case HTTP_REQUEST_TIMEOUT:
            temp = err_template_408;
            html = err_html_408;
            break;

        case HTTP_REQUEST_URI_TOO_LONG:
            temp = err_template_414;
            html = err_html_414;
            break;

        case HTTP_NOTIMPLEMENT:
            temp = err_template_501;
            html = err_html_501;
            break;

        default:    /* HTTP_INTERNAL_SERVER_ERROR */
            temp = err_template_500;
            html = err_html_500;
            break;
    }

    /* 現在時刻をGMTで取得 */
    now_gmtstr(now_date, sizeof(now_date));

    *content_size = (int)strlen(html);
    snprintf(send_buff, sizeof(send_buff), temp,
             now_date, SERVER_NAME, *content_size, html);
    send_data(socket, send_buff, (int)strlen(send_buff));
    return http_status;
}
