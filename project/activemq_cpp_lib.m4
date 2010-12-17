
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
    
dnl
dnl

	activemq_cpp_lib="$with_activemq_cpp_lib_prefix/"
	if test -n "$with_activemq_cpp_lib_prefix" ; then
        	ACTIVEMQ_CPP_LIBS="-L$activemq_cpp_lib -lactivemq-cpp"
	else
		ACTIVEMQ_CPP_LIBS=""
	fi

	activemq_cpp_include="$with_activemq_cpp_include_prefix/"
	if test -n "$with_activemq_cpp_include_prefix" ; then
		ACTIVEMQ_CPP_CFLAGS="-I$with_activemq_cpp_include_prefix"
	else
		ACTIVEMQ_CPP_CFLAGS=""
	fi
dnl
dnl
    AC_SUBST(ACTIVEMQ_CPP_LIBS)
    AC_SUBST(ACTIVEMQ_CPP_CFLAGS)
])
