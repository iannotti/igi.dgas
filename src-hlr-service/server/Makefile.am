## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

noinst_LTLIBRARIES = libdgasXmlHlrHelper.la

noinst_HEADERS = xmlHlrHelper.h

sbin_PROGRAMS = \
	glite-dgas-hlr-listener \
	glite-dgas-hlr-qmgr

libexec_PROGRAMS = \
	glite-dgas-ml-client \
	glite-dgas-hlr-urforward

sbin_SCRIPTS = \
	glite-dgas-hlr-had.pl

if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
DBLIBS = $(UNIXODBC_LIBS)
else
DBFLAGS = $(MYSQL_CFLAGS)
DBLIBS = $(MYSQL_LIBS)
endif

if VOMS
VOMSFLAGS = -DWITH_VOMS
VOMSLIBS = -lvomsapi
endif

libdgasXmlHlrHelper_la_SOURCES = \
	xmlHlrHelper.h \
	xmlHlrHelper.cpp

glite_dgas_hlr_urforward_SOURCES = urForwardFactory.cpp urForwardFactory.h \
	urForward.cpp 

glite_dgas_hlr_urforward_LDADD = \
	$(GLITE_DGAS_COMMON_DBHELPER_LIBS) \
	$(GLOBUS_THR_LIBS) \
	$(GLOBUS_GSS_THR_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(top_builddir)/src/base/libglite_dgas_hlrGenericQuery.la \
	-lglite_dgas_tls_gsisocket_pp \
	$(DBLIBS)
	
glite_dgas_hlr_listener_SOURCES  = hlrServerd.cpp \
	daemonFactory.cpp \
	daemonFactory.h 

glite_dgas_hlr_listener_LDADD =  \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_DBHELPER_LIBS) \
	$(GLOBUS_THR_LIBS) \
	$(GLOBUS_GSS_THR_LIBS) \
	$(GLOBUS_SSL_UTILS_LIBS) \
	$(GLOBUS_LDAP_THR_LIBS) \
	$(SERVICE_BASE_LIBS) \
	./libdgasXmlHlrHelper.la \
	$(top_builddir)/src/engines/libdgasAtmResourceEngine.la \
	$(top_builddir)/src/engines/libdgasAtmResourceEngine2.la \
	$(top_builddir)/src/engines/libdgasEngineCmnUtl.la \
	$(top_builddir)/src/engines/libdgasPingEngine.la \
	$(top_builddir)/src/engines/libdgasAdvancedQueryEngine2.la \
	$(top_builddir)/src/engines/libdgasUrConcentratorEngine.la \
	$(top_builddir)/src/engines/libdgasGetRecordEngine.la \
	$(top_builddir)/src/base/libglite_dgas_serviceCommonUtils.la \
	$(top_builddir)/src/base/libglite_dgas_dbWaitress.la \
	-lglite_dgas_paClient \
	-lglite_dgas_localSecurity \
	-lglite_dgas_tls_gsisocket_pp \
	-lglite_dgas_hlrGenericQuery \
	$(DBLIBS) $(VOMSLIBS)

glite_dgas_ml_client_SOURCES = mlClient.cpp
glite_dgas_ml_client_LDADD   = \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
	$(GLOBUS_GSS_THR_LIBS) \
	$(GLOBUS_SSL_UTILS_LIBS)

glite_dgas_hlr_qmgr_SOURCES = hlrQtransMgrd.cpp
glite_dgas_hlr_qmgr_LDADD = \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_DBHELPER_LIBS) \
	$(GLOBUS_THR_LIBS) \
	$(GLOBUS_GSS_THR_LIBS) \
	$(GLOBUS_SSL_UTILS_LIBS) \
	$(GLOBUS_LDAP_THR_LIBS) \
	$(SERVICE_BASE_LIBS) \
	-lglite_dgas_paClient\
	-lglite_dgas_tls_gsisocket_pp\
	$(top_builddir)/src/engines/libglite_dgas_atmResBankClient.la \
	$(top_builddir)/src/engines/libglite_dgas_atmResBankClient2.la \
	$(top_builddir)/src/engines/libdgasEngineCmnUtl.la \
	$(top_builddir)/src/base/libglite_dgas_serviceCommonUtils.la \
	$(DBLIBS)

SERVICE_BASE_LIBS = \
	$(top_builddir)/src/base/libglite_dgas_hlr.la \
	$(top_builddir)/src/base/libglite_dgas_hlrQTransaction.la \
	$(top_builddir)/src/base/libglite_dgas_hlrTransLog.la 

AM_CPPFLAGS = \
	-I$(top_srcdir)/src/engines \
	-I$(top_srcdir)/src/base \
	-I$(top_srcdir)/interface \
	-I$(top_srcdir)/src \
	$(DBFLAGS) \
	$(GLITE_CFLAGS) \
	$(GLOBUS_THR_CFLAGS) \
	$(VOMSFLAGS) \
	-DGSI -DMERGE 

MAINTAINERCLEANFILES = Makefile.in
