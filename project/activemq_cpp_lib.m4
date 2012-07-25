
AC_DEFUN([AC_ACTIVEMQ_CPP_LIB],
[
	AC_ARG_WITH(activemq_cpp_lib_prefix, 
       [  --with-activemq-cpp-lib-prefix=PFX      prefix where activemq-cpp-lib library is installed.],
       , 
       with_activemq_cpp_lib_prefix=/usr/include/lib/})
	
	AC_ARG_WITH(activemq_cpp_include_prefix, 
       [  --with-activemq-cpp-include-prefix=PFX      prefix where activemq-cpp-lib includes are installed.],
       , 
       with_activemq_cpp_include_prefix=/usr/local/})
	
	AC_ARG_WITH(apr_lib_prefix, 
       [  --with-apr-lib-prefix=PFX      prefix where apr library is installed.],
       , 
       with_apr_lib_prefix=/usr/include/lib/})
	
	AC_ARG_WITH(apr_include_prefix, 
       [  --with-apr-include-prefix=PFX      prefix where apr includes are installed.],
       , 
       with_apr_include_prefix=/usr/local/})
    
dnl
dnl

	activemq_cpp_lib="$with_activemq_cpp_lib_prefix/"
	if test -n "$with_activemq_cpp_lib_prefix" ; then
        	ACTIVEMQ_CPP_LIBS="-L$activemq_cpp_lib -lactivemq-cpp"
	else
		ACTIVEMQ_CPP_LIBS=""
	fi
	
	apr_lib="$with_apr_lib_prefix/"
	if test -n "$with_apr_lib_prefix" ; then
        	APR_LIBS="-L$apr_lib -lapr-1"
	else
		APR_LIBS=""
	fi
	
	aprutil_lib="$with_aprutil_lib_prefix/"
	if test -n "$with_aprutil_lib_prefix" ; then
        	APRUTIL_LIBS="-L$aprutil_lib -laprutil-1"
	else
		APRUTIL_LIBS=""
	fi

	activemq_cpp_include="$with_activemq_cpp_include_prefix/"
	if test -n "$with_activemq_cpp_include_prefix" ; then
		ACTIVEMQ_CPP_CFLAGS="-I$with_activemq_cpp_include_prefix"
	else
		ACTIVEMQ_CPP_CFLAGS=""
	fi
	
	apr_include="$with_apr_include_prefix/"
	if test -n "$with_apr_include_prefix" ; then
		APR_CFLAGS="-I$with_apr_include_prefix"
	else
		APR_CFLAGS=""
	fi
	
	aprutil_include="$with_aprutil_include_prefix/"
	if test -n "$with_aprutil_include_prefix" ; then
		APRUTIL_CFLAGS="-I$with_aprutil_include_prefix"
	else
		APRUTIL_CFLAGS=""
	fi
dnl
dnl
    AC_SUBST(ACTIVEMQ_CPP_LIBS)
    AC_SUBST(APR_LIBS)
    AC_SUBST(APRUTIL_LIBS)
    AC_SUBST(ACTIVEMQ_CPP_CFLAGS)
    AC_SUBST(APRUTIL_CFLAGS)
    AC_SUBST(APR_CFLAGS)
])
