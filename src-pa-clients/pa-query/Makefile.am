## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES  = libglite_dgas_paClient.la
bin_PROGRAMS = dgas-pa-info

libglite_dgas_paClient_la_SOURCES = \
	paClient.cpp 

#libglite_dgas_paClient_la_LIBADD   = \
#	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
#	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
#	-lglite_dgas_tls_gsisocket_pp \
#	$(GLOBUS_SSL_UTILS_LIBS) \
#	$(GLOBUS_GSS_THR_LIBS) 

dgas_pa_info_SOURCES = \
	paInfoClient.cpp

dgas_pa_info_LDADD = \
	libglite_dgas_paClient.la \
	$(GLITE_DGAS_COMMON_TLS_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLOBUS_GSS_THR_LIBS)

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-pa-clients \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/src-common \
	$(GLITE_CFLAGS) \
	$(GLOBUS_THR_CFLAGS) \
	-DGSI

MAINTAINERCLEANFILES = Makefile.in 
