/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* MD5.H - header file for MD5C.C
 */

/* Copyright (C) 1991-2, RSA Data Security, Inc. Created 1991. All
   rights reserved.

   License to copy and use this software is granted provided that it
   is identified as the "RSA Data Security, Inc. MD5 Message-Digest
   Algorithm" in all material mentioning or referencing this software
   or this function.

   License is also granted to make and use derivative works provided
   that such works are identified as "derived from the RSA Data
   Security, Inc. MD5 Message-Digest Algorithm" in all material
   mentioning or referencing the derived work.  
                                                                    
   RSA Data Security, Inc. makes no representations concerning either
   the merchantability of this software or the suitability of this
   software for any particular purpose. It is provided "as is"
   without express or implied warranty of any kind.  
                                                                    
   These notices must be retained in any copies of any part of this
   documentation and/or software.  
 */

#ifndef _MD5_H_
#define _MD5_H_

#ifdef _MSC_VER
#  if _MSC_VER >= 1400 /* Visual Studio 2005 and up */
#    pragma warning(disable:4996) // unsecure
#  endif
#endif

#include "apiexp.h"     /* nestalib API by N.Yamamoto */

/* RSA(global.h) */
/* POINTER defines a generic pointer type */
typedef unsigned char *POINTER;

/* UINT2 defines a two byte word */
typedef unsigned short int UINT2;

/* UINT4 defines a four byte word */
typedef unsigned long int UINT4;

#ifndef NULL_PTR
#define NULL_PTR ((POINTER)0)
#endif

#ifndef UNUSED_ARG
#define UNUSED_ARG(x) x = *(&x);
#endif
/* end of RSA(global.h) */

#ifdef __cplusplus
extern "C" {
#endif

/* MD5 context. */
typedef struct {
  UINT4 state[4];                                   /* state (ABCD) */
  UINT4 count[2];        /* number of bits, modulo 2^64 (lsb first) */
  unsigned char buffer[64];                         /* input buffer */
} RSAMD5_CTX;

APIEXPORT void RSAMD5Init(RSAMD5_CTX*);
APIEXPORT void RSAMD5Update(RSAMD5_CTX*, unsigned char*, unsigned int);
APIEXPORT void RSAMD5Final(unsigned char[16], RSAMD5_CTX*);

/* nestalib API by N.Yamamoto */
APIEXPORT char* md5(char* dst, const char* str);

#ifdef __cplusplus
}
#endif

#endif
