## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES  = libglite_dgas_atmClient.la
libexec_PROGRAMS = glite_dgas_atmClient glite-dgas-sendRecord

libglite_dgas_atmClient_la_SOURCES = \
	atmClient.cpp 

glite_dgas_atmClient_SOURCES = atmClientExe.cpp
glite_dgas_atmClient_LDADD   = \
	./libglite_dgas_atmClient.la \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_TLS_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
	$(GLOBUS_SSL_UTILS_LIBS) \
	$(GLOBUS_GSS_THR_LIBS)

glite_dgas_sendRecord_SOURCES = sendRecordExe.cpp
glite_dgas_sendRecord_LDADD   = \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_TLS_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
	$(GLOBUS_SSL_UTILS_LIBS) \
	$(GLOBUS_GSS_THR_LIBS)

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-hlr-clients \
	-I$(top_srcdir)/src-hlr-clients \
	-I$(top_srcdir)/interface-common \
	$(GLITE_CFLAGS) \
	$(GLOBUS_THR_CFLAGS) \
	-DGSI \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
