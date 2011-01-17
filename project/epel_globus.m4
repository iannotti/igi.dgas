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

   AC_ARG_WITH(globus_thr_flavor,
        [  --with-globus-thr-flavor=flavor [default=null]],
        [],
        with_globus_thr_flavor=${GLOBUS_NOTHR_FLAVOR:-})
    
   AC_ARG_WITH(globus_nothr_flavor,
        [  --with-globus-nothr-flavor=flavor [default=null]],
        [],
        with_globus_nothr_flavor=${GLOBUS_THR_FLAVOR:-})
    
    AC_MSG_RESULT(["with_globus_nothr_flavor=$with_globus_nothr_flavor"])
    AC_MSG_RESULT(["with_globus_thr_flavor=$with_globus_thr_flavor"])

    AC_MSG_RESULT(["GLOBUS lib is $with_globus_lib"])

    GLOBUS_NOTHR_CFLAGS="-I$with_globus_include/ -I$with_globus_lib/globus/include/"
    GLOBUS_THR_CFLAGS="-I$with_globus_include/ -I$with_globus_lib/globus/include/"

    ac_globus_ldlib="-L$with_globus_lib"
	
    if test -n "$with_globus_nothr_flavor"; then
	    GLOBUS_GSS_NOTHR_LIBS="$ac_globus_ldlib -lglobus_gssapi_gsi_$with_globus_nothr_flavor -lglobus_gss_assist_$with_globus_nothr_flavor"
    else
	    GLOBUS_GSS_NOTHR_LIBS="$ac_globus_ldlib -lglobus_gssapi_gsi -lglobus_gss_assist"
    fi
    
    if test -n "$with_globus_thr_flavor"; then		
	    GLOBUS_GSS_THR_LIBS="$ac_globus_ldlib -lglobus_gssapi_gsi_$with_globus_thr_flavor -lglobus_gss_assist_$with_globus_thr_flavor"
    else
	    GLOBUS_GSS_THR_LIBS="$ac_globus_ldlib -lglobus_gssapi_gsi -lglobus_gss_assist"
    fi
    
    AC_MSG_RESULT(["GLOBUS_GSS_THR_LIBS=$GLOBUS_GSS_THR_LIBS"])
    AC_MSG_RESULT(["GLOBUS_GSS_NOTHR_LIBS=$GLOBUS_GSS_NOTHR_LIBS"])

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

