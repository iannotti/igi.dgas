lib_LTLIBRARIES  = \
	libglite_dgas_hlr.la \
	libglite_dgas_hlrQTransaction.la \
	libglite_dgas_hlrGenericQuery.la \
	libglite_dgas_hlrTransLog.la 

noinst_LTLIBRARIES = \
	libglite_dgas_dbWaitress.la \
	libglite_dgas_serviceCommonUtils.la

if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
else
DBFLAGS = $(MYSQL_CFLAGS)
endif

libglite_dgas_dbWaitress_la_SOURCES = \
	dbWaitress.h \
	dbWaitress.cpp

libglite_dgas_serviceCommonUtils_la_SOURCES = \
	serviceCommonUtl.h \
	serviceCommonUtl.cpp 

libglite_dgas_hlr_la_SOURCES = \
	hlrAdmin.cpp \
	hlrResource.cpp \
	hlrTransIn.cpp \
	hlrTransaction.cpp 

libglite_dgas_hlrGenericQuery_la_SOURCES= \
	hlrGenericQuery.cpp

libglite_dgas_hlrQTransaction_la_SOURCES = \
	qTransaction.cpp

libglite_dgas_hlrTransLog_la_SOURCES = \
	transInLog.cpp 

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-hlr-service \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/src-hlr-service \
	$(DBFLAGS) \
	$(GLITE_CFLAGS) \
	-DMERGE


MANTAINERCLEANFILES = Makefile.in
