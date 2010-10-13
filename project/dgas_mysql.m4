dnl Usage:
dnl AC_MYSQL(MINIMUM-VERSION, MAXIMUM-VERSION, [ACTION-IF-FOUND [, ACTION-IF-NOT-FOUND]]])
dnl Test for mysql, and defines
dnl - MYSQL_CFLAGS (compiler flags)
dnl - MYSQL_LIBS (linker flags, stripping and path)
dnl prerequisites:


AC_DEFUN([AC_MYSQL],
[
    AC_ARG_WITH(mysql_prefix, 
       [  --with-mysql-prefix=PFX      prefix where MySQL is installed.],
       , 
       with_mysql_prefix=${MYSQL_INSTALL_PATH:-/usr})

    
    AC_ARG_WITH(mysql_devel_prefix, 
       [  --with-mysql-devel-prefix=PFX      prefix where MySQL devel is installed.],
       , 
       with_mysql_devel_prefix=${MYSQL_INSTALL_PATH:-/usr})

    dnl
    dnl check the mysql version. if the major version is greater than 3
    dnl - then the MYSQL_LIBS macro is equal to -lmysqlclient -lz
    dnl - otherwise the MYSQL_LIBS macro is equal to -lmysqlclient
    dnl

    ac_cv_mysql_valid=no

    ac_save_CFLAGS=$CFLAGS
    ac_save_LIBS=$LIBS

    if test -n "$with_mysql_devel_prefix" -a "$with_mysql_devel_prefix" != "/usr" ; then
	MYSQL_CFLAGS="-I$with_mysql_devel_prefix/include -I$with_mysql_devel_prefix/include/mysql"
        if test "x$HOSTTYPE" = "xx86_64"; then
            MYSQL_LIBS="-L$with_mysql_devel_prefix/lib64 -L$with_mysql_devel_prefix/lib64/mysql"
        else
	        MYSQL_LIBS="-L$with_mysql_devel_prefix/lib -L$with_mysql_devel_prefix/lib/mysql"
        fi
    else
        MYSQL_CFLAGS=""
        MYSQL_LIBS=""
    fi

    if test -n "$with_mysql_prefix" -a "$with_mysql_prefix" != "/usr"; then
        if test "x$HOSTTYPE" = "xx86_64"; then
            MYSQL_LIBS="$MYSQL_LIBS -L$with_mysql_prefix/lib64 -L$with_mysql_prefix/lib64/mysql"
        else
            MYSQL_LIBS="$MYSQL_LIBS -L$with_mysql_prefix/lib -L$with_mysql_prefix/lib/mysql"
        fi
    fi

    MYSQL_LIBS="$MYSQL_LIBS -lmysqlclient -lz"
        
    CFLAGS="$MYSQL_CFLAGS $CFLAGS"
    LIBS="$MYSQL_LIBS $LIBS"

    AC_MSG_RESULT([mysql CFLAGS:  $CFLAGS])
    AC_MSG_RESULT([mysql LIBS:  $LIBS])
        AC_TRY_COMPILE([
  	      #include <mysql.h>
          ],[ MYSQL_FIELD mf ],
          [ ac_cv_mysql_valid=yes ], [ ac_cv_mysql_valid=no ])

        CFLAGS=$ac_save_CFLAGS
        LIBS=$ac_save_LIBS

    AC_MSG_RESULT([mysql status:  $ac_cv_mysql_valid])
    if test "x$ac_cv_mysql_valid" = "xno" ; then
        AC_MSG_RESULT([mysql status: **** suitable version NOT FOUND])
    else
        AC_MSG_RESULT([mysql status: **** suitable version FOUND])
    fi

    if test "x$ac_cv_mysql_valid" = "xyes" ; then
        MYSQL_INSTALL_PATH=$with_mysql_prefix
	ifelse([$3], , :, [$3])
    else
	    MYSQL_CFLAGS=""
	    MYSQL_LIBS=""
	ifelse([$4], , :, [$4])
    fi

    AC_SUBST(MYSQL_INSTALL_PATH)
    AC_SUBST(MYSQL_CFLAGS)
    AC_SUBST(MYSQL_LIBS)
])

