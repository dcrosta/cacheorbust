AC_INIT([cacheorbust], [1.0.0], [dcrosta@late.am])

AM_INIT_AUTOMAKE([-Wall -Werror])

AC_PROG_CXX
AM_PROG_AR

LT_INIT([shared disable-static pic-only dlopen])
AC_ENABLE_SHARED
AC_DISABLE_STATIC

# AC_CHECK_LIB([kyototycoon], [PluggableServer], [], [AC_MSG_ERROR([libkyototycoon not found])]))

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile])

AC_OUTPUT
