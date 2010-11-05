## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES  = libglite_dgas_clientBase.la

libglite_dgas_clientBase_la_SOURCES = \
        dgasClientBase.cpp

AM_CPPFLAGS = \
        -I$(top_srcdir)/interface-hlr-clients \
        -I$(top_srcdir)/interface-common \
        -I$(top_srcdir)/src-hlr-clients \
        $(GLITE_CFLAGS) \
        $(GLOBUS_THR_CFLAGS) \
        -DGSI

MAINTAINERCLEANFILES = Makefile.in
