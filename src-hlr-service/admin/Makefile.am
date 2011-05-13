sbin_PROGRAMS = \
	dgas-hlr-addadmin \
	dgas-hlr-addresource \
	dgas-hlr-queryadmin \
	dgas-hlr-queryresource \
	dgas-hlr-deladmin \
	dgas-hlr-delresource \
	dgas-hlr-checkqueue \
	dgas-hlr-translatedb \
	dgas-hlr-populateJobTransSummary \
	dgas-hlr-sqlexec \
	dgas-hlr-archive

sbin_SCRIPTS = \
	dgas-hlr-dbcreate \
	dgas-hlr-bdiiresimport.pl \
	dgas-sumrecords2goc.pl

libexec_SCRIPTS = \
	dgas-hlr-checkHostCertProxy.sh 

if UNIXODBC
DBFLAGS = $(UNIXODBC_CFLAGS) -DWITH_UNIXODBC
DBLIBS = $(UNIXODBC_LIBS)
else
DBFLAGS = $(MYSQL_CFLAGS)
DBLIBS = $(MYSQL_LIBS)
endif

LDADD = \
	$(GLITE_DGAS_COMMON_CONFIG_LIBS) \
	$(GLITE_DGAS_COMMON_LOG_LIBS) \
	$(GLITE_DGAS_COMMON_XMLUTIL_LIBS) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_dbhelper.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlr.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlrTransLog.la \
	$(top_builddir)/src-hlr-service/base/libdgas_serviceVersion.la \
	$(DBLIBS)

dgas_hlr_archive_SOURCES = hlrArchiveOldRecords.cpp
dgas_hlr_archive_LDADD = \
	$(LDADD) \
	-lglite_dgas_hlrGenericQuery 


dgas_hlr_translatedb_SOURCES = hlrTranslateDb.cpp
dgas_hlr_translatedb_LDADD = \
	$(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_serviceCommonUtils.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_dbWaitress.la \
	-lglite_dgas_hlrGenericQuery -lcrypto 

dgas_hlr_populateJobTransSummary_SOURCES = hlrJTSFeeder.cpp
dgas_hlr_populateJobTransSummary_LDADD = \
	$(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_serviceCommonUtils.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_dbWaitress.la \
	-lglite_dgas_hlrGenericQuery -lcrypto 

dgas_hlr_sqlexec_SOURCES = hlrSqlExec.cpp
dgas_hlr_sqlexec_LDADD = \
	$(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_serviceCommonUtils.la \
	-lglite_dgas_hlrGenericQuery

dgas_hlr_checkqueue_SOURCES = hlrCheckQueue.cpp
dgas_hlr_checkqueue_LDADD = $(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlrQTransaction.la

dgas_hlr_addadmin_SOURCES = hlrAddHlrAdmin.cpp
dgas_hlr_addadmin_LDADD = $(LDADD)

dgas_hlr_addresource_SOURCES = hlrAddResource.cpp
dgas_hlr_addresource_LDADD = $(LDADD)

dgas_hlr_queryadmin_SOURCES = hlrHlrAdminGetInfo.cpp
dgas_hlr_queryadmin_LDADD = $(LDADD)

dgas_hlr_queryresource_SOURCES = hlrRGetInfo.cpp
dgas_hlr_queryresource_LDADD = $(LDADD)

dgas_hlr_deladmin_SOURCES = hlrDelHlrAdmin.cpp
dgas_hlr_deladmin_LDADD = $(LDADD)

dgas_hlr_delresource_SOURCES = hlrDelResource.cpp
dgas_hlr_delresource_LDADD = $(LDADD)


AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-hlr-service \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/src-hlr-service \
	-I$(top_srcdir)/src-hlr-service/engines \
	-I$(top_srcdir)/src-hlr-service/base \
	$(GLITE_CFLAGS) \
	$(DBFLAGS) \
	-DMERGE 

MAINTAINERCLEANFILES = Makefile.in 

