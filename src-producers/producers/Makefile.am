## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES  = libglite_dgas_amqProducer.la \
	libglite_dgas_legacyProducer.la \
	libglite_dgas_legacyCpuComposer.la

libexec_PROGRAMS = glite_dgas_amqProducer \
		glite_dgas_legacyProducer \
	glite_dgas_legacyCpuComposer

libexec_SCRIPTS = \
	glite_dgas_ogfurComposer	


libglite_dgas_amqProducer_la_SOURCES = \
	amqProducer.cpp 

libglite_dgas_legacyProducer_la_SOURCES = \
	legacyProducer.cpp 

libglite_dgas_legacyCpuComposer_la_SOURCES = \
	recordComposer.cpp 

glite_dgas_amqProducer_SOURCES = amqProducerExe.cpp
glite_dgas_amqProducer_LDADD   = \
	./libglite_dgas_amqProducer.la \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) 

glite_dgas_legacyProducer_SOURCES = legacyProducerExe.cpp
glite_dgas_legacyProducer_LDADD   = \
	./libglite_dgas_legacyProducer.la \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
        $(GLOBUS_SSL_UTILS_LIBS) \
        $(GLOBUS_GSS_THR_LIBS)
 

glite_dgas_legacyCpuComposer_SOURCES = recordComposerExe.cpp
glite_dgas_legacyCpuComposer_LDADD   = \
	./libglite_dgas_legacyCpuComposer.la \
	$(ACTIVEMQ_CPP_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) 

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-producers \
	-I$(top_srcdir)/src-producers \
	$(GLITE_CFLAGS) \
	$(ACTIVEMQ_CPP_CFLAGS) \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
