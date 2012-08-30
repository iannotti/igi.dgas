
AC_DEFUN([AC_ACTIVEMQ_CPP_LIB],
[
	AC_ARG_WITH(activemq_cpp_lib_prefix, 
       [  --with-activemq-cpp-lib-prefix=PFX      prefix where activemq-cpp-lib library is installed.],
       , 
       with_activemq_cpp_lib_prefix="")
	
	AC_ARG_WITH(activemq_cpp_include_prefix, 
       [  --with-activemq-cpp-include-prefix=PFX      prefix where activemq-cpp-lib includes are installed.],
       , 
       with_activemq_cpp_include_prefix="")
	
	AC_ARG_WITH(apr_lib_prefix, 
       [  --with-apr-lib-prefix=PFX      prefix where apr library is installed.],
       , 
       with_apr_lib_prefix="")
	
	AC_ARG_WITH(apr_include_prefix, 
       [  --with-apr-include-prefix=PFX      prefix where apr includes are installed.],
       , 
       with_apr_include_prefix="")
       
    AC_ARG_WITH(activemq_cpp, 
       [  --with-activemq-cpp=PFX      path to activemqcpp-config],
       , 
       with_activemq_cpp=/usr/bin/)
       
    AC_ARG_WITH(apr, 
       [  --with-apr=PFX      path to apr-1-config],
       , 
       with_apr=/usr/bin/)
    
dnl
dnl
	activemq_cpp_config="$with_activemq_cpp/activemqcpp-config"
	if test -x $activemq_cpp_config; then
		AC_MSG_RESULT(["Found activemqcpp-config at: $activemq_cpp_config"])
		activemq_cpp_lib=`$activemq_cpp_config --libs`
		activemq_cpp_includes=`$activemq_cpp_config --includes`
		activemq_cpp_cflags=`$activemq_cpp_config --cflags`	
		
	else
		AC_MSG_RESULT(["activemqcpp-config not found within: $with_activemq_cpp"])
	fi

	apr_1_config="$with_apr/apr-1-config"
	if test -x $apr_1_config; then
		AC_MSG_RESULT(["Found apr-1-config at: $apr_1_config"])
		apr_lib=`$apr_1_config --libs`
		apr_includes=`$apr_1_config --includes`
		apr_cflags=`$apr_1_config --cflags`	
	else
		AC_MSG_RESULT(["apr-1-config not found within: $with_apr"])
	fi

	if test -n "$with_activemq_cpp_lib_prefix" ; then
        ACTIVEMQ_CPP_LIBS="-L$with_activemq_cpp_lib_prefix -lactivemq-cpp"
	else
		ACTIVEMQ_CPP_LIBS="$activemq_cpp_lib"
	fi
	
	if test -n "$with_activemq_cpp_include_prefix" ; then
		ACTIVEMQ_CPP_CFLAGS="-I$with_activemq_cpp_include_prefix/"
	else
		ACTIVEMQ_CPP_CFLAGS="$activemq_cpp_cflags $activemq_cpp_includes"
	fi
	
	if test -n "$with_apr_lib_prefix" ; then
        APR_LIBS="-L$apr_lib -lapr-1"
	else
		APR_LIBS="$apr_lib"
	fi
	
	if test -n "$with_apr_include_prefix" ; then
		APR_CFLAGS="-I$with_apr_include_prefix"
	else
		APR_CFLAGS="$apr_cflags $apr_includes"
	fi
	
	aprutil_lib="$with_aprutil_lib_prefix/"
	if test -n "$with_aprutil_lib_prefix" ; then
        APRUTIL_LIBS="-L$aprutil_lib -laprutil-1"
	else
		APRUTIL_LIBS=""
	fi

	aprutil_include="$with_aprutil_include_prefix/"
	if test -n "$with_aprutil_include_prefix" ; then
		APRUTIL_CFLAGS="-I$with_aprutil_include_prefix"
	else
		APRUTIL_CFLAGS=""
	fi

AC_MSG_RESULT(["USING ACTIVEMQ_CPP_LIBS: $ACTIVEMQ_CPP_LIBS"])
AC_MSG_RESULT(["USING ACTIVEMQ_CPP_CFLAGS: $ACTIVEMQ_CPP_CFLAGS"])
AC_MSG_RESULT(["USING APR_LIBS: $APR_LIBS"])
AC_MSG_RESULT(["USING APR_CFLAGS: $APR_CFLAGS"])

dnl
dnl
    AC_SUBST(ACTIVEMQ_CPP_LIBS)
    AC_SUBST(APR_LIBS)
    AC_SUBST(APRUTIL_LIBS)
    AC_SUBST(ACTIVEMQ_CPP_CFLAGS)
    AC_SUBST(APRUTIL_CFLAGS)
    AC_SUBST(APR_CFLAGS)
])
