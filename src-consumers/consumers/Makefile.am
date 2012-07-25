## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************


lib_LTLIBRARIES  = \
		libglite_dgas_legacyRecordManager.la 

libexec_PROGRAMS = dgas-AMQConsumer \
		dgas-legacyRecordManager 

libexec_SCRIPTS = \
		dgas-ogfurParser \
		dgas-emicarParser	

if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
DBLIBS = $(UNIXODBC_LIBS)
else
DBFLAGS = $(MYSQL_CFLAGS)
DBLIBS = $(MYSQL_LIBS)
endif


dgas_AMQConsumer_SOURCES = AMQConsumerExe.cpp
dgas_AMQConsumer_LDADD   = \
	$(top_builddir)/src-msg-common/libdgas_asyncConsumer.la \
	$(top_builddir)/src-msg-common/libdgas_amqConsumer.la \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_dbhelper.la \
	$(top_builddir)/src-hlr-service/base/libdgas_serviceVersion.la \
	$(GLITE_DGAS_COMMON_DBHELPER_LIBS) \
	$(DBLIBS) 

libglite_dgas_legacyRecordManager_la_SOURCES = \
	legacyRecordManager.cpp 

dgas_legacyRecordManager_SOURCES = legacyRecordManagerExe.cpp
dgas_legacyRecordManager_LDADD   = \
	./libglite_dgas_legacyRecordManager.la \
	$(top_builddir)/src-hlr-service/engines/libdgasAtmResourceEngine.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlr.la \
	$(top_builddir)/src-hlr-service/engines/libdgasEngineCmnUtl.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlrTransLog.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlrQTransaction.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_serviceCommonUtils.la \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_DBHELPER_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(DBLIBS) 

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/interface-consumers \
	-I$(top_srcdir)/interface-hlr-service \
	-I$(top_srcdir)/interface-msg-common \
	-I$(top_srcdir)/src-consumers \
	$(GLITE_CFLAGS) \
	$(DBFLAGS) \
	$(ACTIVEMQ_CPP_CFLAGS) \
	$(APR_CFLAGS) \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
