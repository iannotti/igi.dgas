## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

man1_MANS = dgas-ping.man

lib_LTLIBRARIES  = libglite_dgas_pingClient.la
bin_PROGRAMS = dgas-ping

libglite_dgas_pingClient_la_SOURCES = \
	pingClient.cpp 

dgas_ping_SOURCES = pingClientExe.cpp
dgas_ping_LDADD   = \
	./libglite_dgas_pingClient.la \
	$(GLITE_DGAS_COMMON_TLS_LIBS) \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	-lglite_dgas_tls_gsisocket_pp \
	$(GLOBUS_SSL_UTILS_LIBS) \
	$(GLOBUS_GSS_THR_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS)

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/interface-hlr-clients \
	-I$(top_srcdir)/src-hlr-clients \
	$(GLITE_CFLAGS) \
	$(GLOBUS_THR_CFLAGS) \
	-DGSI

MAINTAINERCLEANFILES = Makefile.in 
