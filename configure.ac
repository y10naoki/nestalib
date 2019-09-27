#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(nestalib, 0.9.5, y10naoki@gmail.com)
AM_INIT_AUTOMAKE($PACKAGE_NAME, $PACKAGE_VERSION)
AC_CONFIG_SRCDIR([nio.c])
AC_CONFIG_HEADER([config.h])

# Initialize prefix.
if test "$prefix" = "NONE"; then
   prefix="/usr/local"
fi

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
AC_PROG_LIBTOOL

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h limits.h memory.h netdb.h netinet/in.h stdlib.h string.h sys/socket.h sys/timeb.h unistd.h])

# checks for args.
AC_ARG_WITH([libxml],
    [  --with-libxml=path location of the libxml headers (default is /usr/include/libxml2))],
    [LIBXML2_HEADERS=${withval}],
    [LIBXML2_HEADERS=/usr/include/libxml2])
AC_SUBST([LIBXML2_HEADERS])

AC_ARG_WITH([pgsql],
    [  --with-pgsql=path location of the PostgreSQL headers],
    [PGSQL_HEADERS=${withval}],
    [PGSQL_HEADERS=.])
AC_SUBST([PGSQL_HEADERS])
if test "$PGSQL_HEADERS" != "."; then
    AC_DEFINE([HAVE_PGSQL], 1, [Define supports the PostgreSQL], [])
fi

AC_ARG_WITH([oci],
    [  --with-oci=path location of the Oracle OCI headers],
    [OCI_HEADERS=${withval}],
    [OCI_HEADERS=.])
AC_SUBST([OCI_HEADERS])
if test "$OCI_HEADERS" != "."; then
    AC_DEFINE([HAVE_ORACLE_OCI], 1, [Define supports the Oracle OCI], [])
fi

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM

# Checks for library functions.
AC_FUNC_ALLOCA
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([clock_gettime gethostbyaddr gethostbyname getpagesize gettimeofday inet_ntoa memmove memset munmap select socket strchr strerror strstr])
AC_CHECK_FUNCS([iconv])
AC_CHECK_FUNC([epoll_create],
  [AC_DEFINE([HAVE_EPOLL], 1, [Define if your system supports the epoll system call])], [])
AC_CHECK_FUNC([kqueue],
  [AC_DEFINE([HAVE_KQUEUE], 1, [Define if your system supports the kqueue system call])], [])

# check optional
AC_CHECK_LIB([iconv], [iconv],
  [AC_DEFINE([HAVE_ICONV], 1, [Define if your system supports the libiconv])])
AC_CHECK_LIB([z], [deflate],
  [AC_DEFINE([HAVE_ZLIB], 1, [Define if your system supports the libz])]) 
AC_CHECK_LIB([xml2], [xmlReadMemory],
  [AC_DEFINE([HAVE_LIBXML2], 1, [Define if your system supports the LIBXML2])])
AC_CHECK_LIB([ssl], [SSL_library_init],
  [AC_DEFINE([HAVE_OPENSSL], 1, [Define if your system supports the OPENSSL])])

AC_CHECK_LIB([clntsh], [OCIInitialize])
AC_CHECK_LIB([pq], [PQsetdbLogin])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
