#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(CIRRUS, 0.1, joao@berkeley.edu)
AM_INIT_AUTOMAKE([subdir-objects])

# Checks for programs.
AC_PROG_CXX([mpic++])
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AX_CXX_COMPILE_STDCXX_14

CXXFLAGS="-O3 -fPIC -std=c++17"

# Checks for header files.
AC_CHECK_HEADERS([ arpa/inet.h netdb.h string.h stdint.h\
        stdlib.h sys/types.h sys/socket.h sys/epoll.h\
        sys/time.h syslog.h unistd.h google/dense_hash_map])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_STRCOLL
AC_CHECK_FUNCS([gettimeofday localtime_r memchr memmove memset mkdir stpcpy strchr strcspn strdup strerror strpbrk strrchr strspn strstr])
AC_CONFIG_FILES([Makefile
                 src/Makefile
                 tests/Makefile
                 tests/iterator/Makefile
                 tests/test_s3/Makefile
                 tests/test_travis/Makefile
                 tests/test_travis_lr/Makefile
                 tests/test_travis_mf/Makefile])

AC_OUTPUT
