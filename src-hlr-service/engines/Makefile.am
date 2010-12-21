## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

noinst_LTLIBRARIES = \
	libdgasGetRecordEngine.la \
	libdgasPingEngine.la \
	libdgasAdvancedQueryEngine2.la \
	libdgasUrConcentratorEngine.la
noinst_HEADERS = \
	pingEngine.hpp \
	advancedQueryEngine2.h 

lib_LTLIBRARIES = \
		libdgasEngineCmnUtl.la \
		libdgasAtmResourceEngine.la \
		libdgasAtmResourceEngine2.la \
		libglite_dgas_atmResBankClient.la \
 		libglite_dgas_atmResBankClient2.la 

if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
else
DBFLAGS = $(MYSQL_CFLAGS)
endif

if VOMS
VOMSFLAGS = -DWITH_VOMS
endif

libdgasEngineCmnUtl_la_SOURCES = engineCmnUtl.cpp engineCmnUtl.h

libdgasPingEngine_la_SOURCES = pingEngine.cpp pingEngine.hpp

libdgasAtmResourceEngine_la_SOURCES = atmResourceEngine.cpp atmResourceEngine.h

libdgasAtmResourceEngine2_la_SOURCES = atmResourceEngine2.cpp atmResourceEngine2.h

libdgasGetRecordEngine_la_SOURCES = getRecordEngine.cpp getRecordEngine.h

libdgasUrConcentratorEngine_la_SOURCES = urConcentratorEngine.h urConcentratorEngine.cpp

libdgasAdvancedQueryEngine2_la_SOURCES = advancedQueryEngine2.h advancedQueryEngine2.cpp

libglite_dgas_atmResBankClient_la_SOURCES = \
	atmResBankClient.h atmResBankClient.cpp
#libglite_dgas_atmResBankClient_la_LIBADD = libdgasEngineCmnUtl.la

libglite_dgas_atmResBankClient2_la_SOURCES = \
	atmResBankClient2.h atmResBankClient2.cpp
#libglite_dgas_atmResBankClient2_la_LIBADD = libdgasEngineCmnUtl.la

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-hlr-service \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/src-hlr-service \
	-I$(top_srcdir)/src-hlr-service/engines \
	-I$(top_srcdir)/src-hlr-service/base \
	$(GLITE_CFLAGS) \
	$(GLOBUS_THR_CFLAGS) \
	$(DBFLAGS) \
	-DGSI \
	-DVERSION=\"@VERSION@\" \
	$(VOMSFLAGS) \
	-DMERGE 

MAINTAINERCLEANFILES = Makefile.in
