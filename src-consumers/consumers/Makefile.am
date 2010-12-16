## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES  = libglite_dgas_AMQConsumer.la \
		libglite_dgas_AMQRecordManager.la 

libexec_PROGRAMS = glite_dgas_AMQConsumer \
		glite_dgas_AMQRecordManager 

libexec_SCRIPTS = \
		glite_dgas_ogfurParser	

if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
DBLIBS = $(UNIXODBC_LIBS)
else
DBFLAGS = $(MYSQL_CFLAGS)
DBLIBS = $(MYSQL_LIBS)
endif

libglite_dgas_AMQConsumer_la_SOURCES = \
	AMQConsumer.cpp 

glite_dgas_AMQConsumer_SOURCES = AMQConsumerExe.cpp
glite_dgas_AMQConsumer_LDADD   = \
	./libglite_dgas_AMQConsumer.la \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) 

libglite_dgas_AMQRecordManager_la_SOURCES = \
	AMQRecordManager.cpp 

glite_dgas_AMQRecordManager_SOURCES = AMQRecordManagerExe.cpp
glite_dgas_AMQRecordManager_LDADD   = \
	./libglite_dgas_AMQRecordManager.la \
	-ldgasAtmResourceEngine \
	-lglite_dgas_hlr \
	-lglite_dgas_hlrQTransaction \
	-lglite_dgas_hlrTransLog \
	-ldgasEngineCmnUtl \
	-lglite_dgas_serviceCommonUtils \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_LOCK_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_DBHELPER_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(DBLIBS) 

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface \
	-I$(top_srcdir)/src \
	$(GLITE_CFLAGS) \
	$(ACTIVEMQ_CPP_CFLAGS) \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
