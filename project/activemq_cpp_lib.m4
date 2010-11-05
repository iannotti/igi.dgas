dnl Usage:
dnl AC_GLITE_DGAS_COMMON



AC_DEFUN([AC_ACTIVEMQ_CPP_LIB],
[
	AC_ARG_WITH(activemq_cpp_lib_prefix, 
       [  --with-activemq-cpp-lib-prefix=PFX      prefix where MySQL devel is installed.],
       , 
       with_activemq_cpp_lib_prefix=/usr/})
    
    if test "x$host_cpu" = "xx86_64"; then
       	activemq_cpp_lib="$with_activemq_cpp_lib_prefix/lib64"
    else
    	activemq_cpp_lib="$with_activemq_cpp_lib_prefix/lib" 
    fi

    if test -n "$with_activemq_cpp_lib_prefix" ; then
	dnl
	dnl
	dnl
        ACTIVEMQ_CPP_LIBS="-L$activemq_cpp_lib -lactivemq-cpp"
	ACTIVEMQ_CPP_CFLAGS="-I$with_activemq_cpp_lib_prefix/include/activemq-cpp-3.1.0"
    else
	ACTIVEMQ_CPP_LIBS=""
    fi

    AC_SUBST(ACTIVEMQ_CPP_LIBS)
    AC_SUBST(ACTIVEMQ_CPP_CFLAGS)
])
