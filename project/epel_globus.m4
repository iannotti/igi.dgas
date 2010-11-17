dnl Usage:
dnl AC_GLOBUS(MINIMUM-VERSION, [ACTION-IF-FOUND [, ACTION-IF-NOT-FOUND]]])
dnl - GLOBUS_LOCATION
dnl - GLOBUS_NOTHR_FLAVOR
dnl - GLOBUS_THR_FLAVOR
dnl - GLOBUS_NOTHR_CFLAGS
dnl - GLOBUS_THR_CFLAGS
dnl - GLOBUS_NOTHR_LIBS
dnl - GLOBUS_THR_LIBS
dnl - GLOBUS_COMMON_NOTHR_LIBS
dnl - GLOBUS_COMMON_THR_LIBS
dnl - GLOBUS_STATIC_COMMON_NOTHR_LIBS
dnl - GLOBUS_STATIC_COMMON_THR_LIBS
dnl - GLOBUS_FTP_CLIENT_NOTHR_LIBS
dnl - GLOBUS_FTP_CLIENT_THR_LIBS
dnl - GLOBUS_SSL_NOTHR_LIBS
dnl - GLOBUS_SSL_THR_LIBS
dnl - GLOBUS_GSS_NOTHR_LIBS
dnl - GLOBUS_GSS_THR_LIBS
dnl - GLOBUS_LDAP_THR_LIBS

AC_DEFUN([AC_GLOBUS_EPEL],
[
    AC_ARG_WITH(globus_include,
    [  --with-globus-include=include-path [default=/usr/include/globus]],
    [],
        with_globus_include=${GLOBUS_INCLUDE:-/usr/include/globus/})

    AC_MSG_RESULT(["GLOBUS include is $with_globus_include"])

    AC_ARG_WITH(globus_lib,
        [  --with-globus-lib=lib-path [default=/usr/lib64]],
        [],
        with_globus_lib=${GLOBUS_LIB:-/usr/lib64/})

    AC_MSG_RESULT(["GLOBUS lib is $with_globus_lib"])

    GLOBUS_NOTHR_CFLAGS="-I$with_globus_include/ -I$with_globus_lib/globus/include/"
    GLOBUS_THR_CFLAGS="-I$with_globus_include/ -I$with_globus_lib/globus/include/"

    ac_globus_ldlib="-L$with_globus_lib"

    GLOBUS_GSS_NOTHR_LIBS="$ac_globus_ldlib -lglobus_gssapi_gsi -lglobus_gss_assist"
    GLOBUS_GSS_THR_LIBS="$ac_globus_ldlib -lglobus_gssapi_gsi -lglobus_gss_assist"


    dnl
    dnl check whether globus in place, if not return error
    dnl
    AC_MSG_CHECKING([for globus version])
    grep GLOBUS_VERSION= $with_globus_prefix/bin/globus-version | cut -d'"' -f2 >& globus.version
    ac_globus_version=`cat globus.version`
    ac_globus_point_version=`cut -d. -f3 globus.version`
    ac_globus_minor_version=`cut -d. -f2 globus.version`
    ac_globus_major_version=`cut -d. -f1 globus.version`

    if test -n "$ac_globus_point_version" ; then
        AC_MSG_RESULT([$ac_globus_version])
    else
        GLOBUS_NOTHR_CFLAGS=""
        GLOBUS_THR_CFLAGS=""
        GLOBUS_NOTHR_LIBS=""
        GLOBUS_THR_LIBS=""
        GLOBUS_SSL_NOTHR_LIBS=""
        GLOBUS_SSL_THR_LIBS=""
        echo ac_point_version $ac_globus_point_version
        AC_MSG_ERROR([no])
    fi

    AC_SUBST(GLOBUS_LOCATION)
    AC_SUBST(GLOBUS_NOTHR_CFLAGS)
    AC_SUBST(GLOBUS_THR_CFLAGS)
    AC_SUBST(GLOBUS_NOTHR_LIBS)
    AC_SUBST(GLOBUS_THR_LIBS)
    AC_SUBST(GLOBUS_COMMON_NOTHR_LIBS)
    AC_SUBST(GLOBUS_COMMON_THR_LIBS)
    AC_SUBST(GLOBUS_GSS_NOTHR_LIBS)
    AC_SUBST(GLOBUS_GSS_THR_LIBS)
])

