## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

lib_LTLIBRARIES = \
	libglite_dgas_xmlutil.la \
	libglite_dgas_config.la \
	libglite_dgas_log.la \
	libglite_dgas_localSecurity.la \
	libglite_dgas_lock.la

libglite_dgas_config_la_SOURCES = dgas_config.cpp

libglite_dgas_log_la_SOURCES = libdgas_log.cpp

libglite_dgas_xmlutil_la_SOURCES = xmlUtil.cpp

libglite_dgas_lock_la_SOURCES = dgas_lock.cpp

libglite_dgas_localSecurity_la_SOURCES = localSecurity.cpp


if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
else
DBFLAGS = $(MYSQL_CFLAGS)
endif

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-common \
	$(DBFLAGS)

MAINTAINERCLEANFILES = Makefile.in
