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

AC_DEFUN([AC_GLITE_DGAS_COMMON],
[
dnl    ac_glite_dgas_common_prefix=$GLITE_LOCATION
dnl    if test "x$host_cpu" = "xx86_64"; then
dnl       	ac_glite_dgas_common_lib="-L$ac_glite_dgas_common_prefix/lib64"
dnl    else
dnl    	ac_glite_dgas_common_lib="-L$ac_glite_dgas_common_prefix/lib" 
dnl    fi

	AC_MSG_RESULT([dgas_common LIB flag:  $ac_glite_dgas_common_lib])


	dnl
	dnl
	dnl
        GLITE_DGAS_COMMON_DBHELPER_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_dbhelper"
	GLITE_DGAS_COMMON_CONFIG_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_config"
	GLITE_DGAS_COMMON_DIGESTS_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_digests"
	GLITE_DGAS_COMMON_LOG_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_log"
	GLITE_DGAS_COMMON_GRIDMAPFILE_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_gridMapFile"
	GLITE_DGAS_COMMON_LOCK_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_lock"
	GLITE_DGAS_COMMON_XMLUTIL_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_xmlutil"
	GLITE_DGAS_COMMON_LDAP_LIBS="-L$WORKDIR/src-common/base/ -lglite_dgas_ldap"
	GLITE_DGAS_COMMON_TLS_LIBS="-L$WORKDIR/src-common/tls/socket++ -lglite_dgas_tls_gsisocket_pp"

    AC_SUBST(GLITE_DGAS_COMMON_DBHELPER_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_CONFIG_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_DIGESTS_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_LOG_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_GRIDMAPFILE_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_LOCK_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_XMLUTIL_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_LDAP_LIBS)
    AC_SUBST(GLITE_DGAS_COMMON_TLS_LIBS)
])
