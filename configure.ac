#       Copyright (c) 2004 on behalf of the EU EGEE Project:
#       The European Organization for Nuclear Research (CERN),
#       Istituto Nazionale di Fisica Nucleare (INFN), Italy
#       Datamat Spa, Italy
#       Centre National de la Recherche Scientifique (CNRS), France
#       CS Systeme d'Information (CSSI), France
#       Royal Institute of Technology, Center for Parallel Computers (KTH-PDC), Sweden
#       Universiteit van Amsterdam (UvA), Netherlands
#       University of Helsinki (UH.HIP), Finland
#       University of Bergen (UiB), Norway
#       Council for the Central Laboratory of the Research Councils (CCLRC), United Kingdom
#
#t                                                                                                                                                             
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT([DGAS], [4.2.0b])
AC_CONFIG_AUX_DIR([./project])
AM_INIT_AUTOMAKE([1.6.3 subdir-objects])
AC_CONFIG_SRCDIR(src-common/base/dgas_config.cpp)

# Notices.
AC_COPYRIGHT([Copyright (c) 2004 The EU EGEE Project
See LICENCE file for details
])
AC_REVISION([$Revision: 1.1.2.5.4.33.2.1 $])

#Environment.
WORKDIR=`pwd`
AC_SUBST(WORKDIR)

# Checks for programs.
AC_PROG_CC
AC_PROG_CPP
AC_PROG_CXX
AC_PROG_CXXCPP
AM_PROG_CC_C_O
AC_PROG_LIBTOOL

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_UID_T
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_HEADER_STDC
AC_FUNC_MEMCMP
AC_FUNC_STAT
AC_FUNC_STRFTIME
AC_FUNC_VPRINTF
AC_CHECK_FUNCS([bzero memset mkdir strtol])

have_globus=no
have_mysql=no
have_unixodbc=no

AC_ARG_ENABLE(unixodbc,
	[  --enable-unixodbc    Uses unixODBC],
	[case "${enableval}" in
	  yes) AC_UNIXODBC([2.2.11], have_unixodbc=yes, have_unixodbc=no);;
	  no)  AC_MYSQL([4.0.12], [4.1.20], have_mysql=yes, have_mysql=no);;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-unixodbc) ;;
	esac],[AC_MYSQL([4.0.12], [4.1.20], have_mysql=yes, have_mysql=no)])

AM_CONDITIONAL(UNIXODBC, test x$have_unixodbc = xyes)

AC_ARG_ENABLE(voms,
	[  --enable-voms    Uses VOMS],
	[case "${enableval}" in
	  yes) have_voms=yes;;
	  no)  have_voms=no;;
	*) AC_MSG_ERROR(bad value ${enableval} for --enable-voms) ;;
	esac],[have_voms=yes])

AM_CONDITIONAL(VOMS, test x$have_voms = xyes)

AC_GLOBUS_EPEL([], have_globus=yes, have_globus=no)
AC_MSG_RESULT(["GLOBUS found $have_globus"])
AC_MSG_RESULT(["UNIXODBC found $have_unixodbc"])
AC_MSG_RESULT(["MySQL found $have_mysql"])
AC_MSG_RESULT(["uses VOMS = $have_voms"])

AC_GLITE

AC_GLITE_DGAS_PA_CLIENTS([], have_glite_dgas_pa_clients=yes, have_glite_dgas_pa_clients=no)
AC_GLITE_DGAS_COMMON([], have_glite_dgas_common=yes, have_glite_dgas_common=no)
AC_MSG_RESULT(["GLITE_DGAS_COMMON found $have_glite_dgas_common"])

AC_ACTIVEMQ_CPP_LIB([], have_activemq_cpp_lib=yes, have_activemq_cpp_lib=no)
AC_MSG_RESULT(["ACTIVEMQ_CPP_LIB found $have_activemq_cpp_lib"])

#CXXFLAGS="-g -Wall -O0 -pg -fprofile-arcs -ftest-coverage"
#CFLAGS="-g -O0 -Wall -W -pg -fprofile-arcs -ftest-coverage"
#LDFLAGS="-fprofile-arcs -ftest-coverage -lgcov"
#AC_SUBST(CXXFLAGS)
#AC_SUBST(CFLAGS)
#AC_SUBST(LDFLAGS)

#CXXFLAGS="-g -Wall -O0 -pg -fprofile-arcs -fbranch-probabilities"
#CFLAGS="-g -O0 -Wall -W -pg -fprofile-arcs -fbranch-probabilities"
#LDFLAGS="-fprofile-arcs -fbranch-probabilities -lgcov"
#AC_SUBST(CXXFLAGS)
#AC_SUBST(CFLAGS)
#AC_SUBST(LDFLAGS)

# Configuration items
AC_PREFIX_DEFAULT([/usr])
AC_CONFIG_FILES([interface-common/Makefile])
AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([src-common/Makefile])
AC_CONFIG_FILES([src-common/base/Makefile])
AC_CONFIG_FILES([src-common/tls/Makefile])
AC_CONFIG_FILES([src-common/tls/socket++/Makefile])
AC_CONFIG_FILES([interface-msg-common/Makefile])
AC_CONFIG_FILES([src-msg-common/Makefile])
AC_CONFIG_FILES([interface-hlr-clients/Makefile])
AC_CONFIG_FILES([src-hlr-clients/Makefile])
AC_CONFIG_FILES([src-hlr-clients/ping/Makefile])
AC_CONFIG_FILES([src-hlr-clients/ui/Makefile])
AC_CONFIG_FILES([src-hlr-clients/base/Makefile])
AC_CONFIG_FILES([src-sensors/Makefile])
AC_CONFIG_FILES([src-sensors/base/Makefile])
AC_CONFIG_FILES([src-sensors/atm/Makefile])
AC_CONFIG_FILES([src-sensors/ce_daemons/Makefile])
AC_CONFIG_FILES([interface-sensors/Makefile])
AC_CONFIG_FILES([config-sensors/Makefile])
AC_CONFIG_FILES([config-hlr-service/Makefile])
AC_CONFIG_FILES([interface-hlr-service/Makefile])
AC_CONFIG_FILES([src-hlr-service/Makefile])
AC_CONFIG_FILES([src-hlr-service/base/Makefile])
AC_CONFIG_FILES([src-hlr-service/admin/Makefile])
AC_CONFIG_FILES([src-hlr-service/engines/Makefile])
AC_CONFIG_FILES([src-hlr-service/server/Makefile])
AC_CONFIG_FILES([src-hlr-service/regression/Makefile])
AC_CONFIG_FILES([src-hlr-service/regression/tests/Makefile])
AC_CONFIG_FILES([src-hlr-service/monitor/Makefile])
AC_CONFIG_FILES([src-hlr-service/monitor/plugins/Makefile])
AC_CONFIG_FILES([config-producers/Makefile])
AC_CONFIG_FILES([src-producers/Makefile])
AC_CONFIG_FILES([src-producers/producers/Makefile])
AC_CONFIG_FILES([interface-producers/Makefile])
AC_CONFIG_FILES([config-consumers/Makefile])
AC_CONFIG_FILES([src-consumers/Makefile])
AC_CONFIG_FILES([src-consumers/consumers/Makefile])
AC_CONFIG_FILES([interface-consumers/Makefile])
AC_OUTPUT

