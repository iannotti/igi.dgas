## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES  = \
	libdgas_legacyProducer.la \
	libdgas_legacyCpuComposer.la

libexec_PROGRAMS = dgas-amqProducer \
		dgas-legacyProducer \
		dgas-legacyStorageComposer \
		dgas-legacyCpuComposer

libexec_SCRIPTS = \
	dgas-ogfurComposer	\
	dgas-emicarComposer \
	dgas-record-filter.pl \
	dgas-stompProducer.pl \
	dgas-sqlInsertComposer.pl


libdgas_legacyProducer_la_SOURCES = \
	legacyProducer.cpp 

libdgas_legacyCpuComposer_la_SOURCES = \
	legacyComposer.cpp 

dgas_amqProducer_SOURCES = amqProducerExe.cpp
dgas_amqProducer_LDADD   = \
	$(top_builddir)/src-msg-common/libdgas_amqProducer.la \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) 

dgas_legacyProducer_SOURCES = legacyProducerExe.cpp
dgas_legacyProducer_LDADD   = \
	./libdgas_legacyProducer.la \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_TLS_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
        $(GLOBUS_SSL_UTILS_LIBS) \
        $(GLOBUS_GSS_THR_LIBS)
 

dgas_legacyCpuComposer_SOURCES = legacyComposerExe.cpp
dgas_legacyCpuComposer_LDADD   = \
	./libdgas_legacyCpuComposer.la \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) 
	
dgas_legacyStorageComposer_SOURCES = legacyStorageComposerExe.cpp
dgas_legacyStorageComposer_LDADD   = \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS)


AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-producers \
	-I$(top_srcdir)/interface-common\
	-I$(top_srcdir)/src-producers \
	-I$(top_srcdir)/interface-msg-common \
	$(GLITE_CFLAGS) \
	$(ACTIVEMQ_CPP_CFLAGS) \
	$(GLOBUS_THR_CFLAGS) \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
