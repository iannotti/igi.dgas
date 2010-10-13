dnl Usage:
dnl AC_GLITE_DGAS_COMMON
dnl - GLITE_DGAS_COMMON_DBHELPER_LIBS
dnl - GLITE_DGAS_COMMON_CONFIG_LIBS
dnl - GLITE_DGAS_COMMON_DIGESTS_LIBS
dnl - GLITE_DGAS_COMMON_LOG_LIBS
dnl - GLITE_DGAS_COMMON_GRIDMAPFILE_LIBS
dnl - GLITE_DGAS_COMMON_LOCK_LIBS
dnl - GLITE_DGAS_COMMON_XMLUTIL_LIBS
dnl - GLITE_DGAS_COMMON_LDAP_LIBS
dnl - GLITE_DGAS_COMMON_TLS_LIBS

AC_DEFUN([AC_GLITE_DGAS_PA_CLIENTS],
[
dnl    ac_glite_dgas_common_prefix=$GLITE_LOCATION
dnl    if test "x$host_cpu" = "xx86_64"; then
dnl       	ac_glite_dgas_common_lib="-L$ac_glite_dgas_common_prefix/lib64"
dnl    else
dnl    	ac_glite_dgas_common_lib="-L$ac_glite_dgas_common_prefix/lib" 
dnl    fi



	dnl
	dnl
	dnl
        GLITE_DGAS_PACLIENTS_LIBS="-L$WORKDIR/src-pa-clients/pa-query/ "
AC_MSG_RESULT([dgas_pa_clients LIB in:  $GLITE_DGAS_PACLIENTS_LIBS])
    AC_SUBST(GLITE_DGAS_PACLIENTS_LIBS)
])
