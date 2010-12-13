sbin_PROGRAMS = \
	glite-dgas-hlr-addadmin \
	glite-dgas-hlr-addresource \
	glite-dgas-hlr-queryadmin \
	glite-dgas-hlr-queryresource \
	glite-dgas-hlr-deladmin \
	glite-dgas-hlr-delresource \
	glite-dgas-hlr-checkqueue \
	glite-dgas-hlr-translatedb \
	glite-dgas-hlr-sqlexec \
	glite-dgas-hlr-archive

sbin_SCRIPTS = \
	glite-dgas-clean-wJobs \
	glite-dgas-hlrd-dbcreate \
	glite-dgas-hlr-bdiiresimport.pl \
	glite-dgas-sumrecords2goc.pl

libexec_SCRIPTS = \
	glite-dgas-hlr-checkHostCertProxy.sh 

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
	$(DBLIBS)

glite_dgas_hlr_archive_SOURCES = hlrArchiveOldRecords.cpp
glite_dgas_hlr_archive_LDADD = \
	$(LDADD) \
	-lglite_dgas_hlrGenericQuery 


glite_dgas_hlr_translatedb_SOURCES = hlrTranslateDb.cpp
glite_dgas_hlr_translatedb_LDADD = \
	$(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_serviceCommonUtils.la \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_dbWaitress.la \
	-lglite_dgas_hlrGenericQuery -lcrypto 

glite_dgas_hlr_sqlexec_SOURCES = hlrSqlExec.cpp
glite_dgas_hlr_sqlexec_LDADD = \
	$(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_serviceCommonUtils.la \
	-lglite_dgas_hlrGenericQuery

glite_dgas_hlr_checkqueue_SOURCES = hlrCheckQueue.cpp
glite_dgas_hlr_checkqueue_LDADD = $(LDADD) \
	$(top_builddir)/src-hlr-service/base/libglite_dgas_hlrQTransaction.la

glite_dgas_hlr_addadmin_SOURCES = hlrAddHlrAdmin.cpp
glite_dgas_hlr_addadmin_LDADD = $(LDADD)

glite_dgas_hlr_addresource_SOURCES = hlrAddResource.cpp
glite_dgas_hlr_addresource_LDADD = $(LDADD)

glite_dgas_hlr_queryadmin_SOURCES = hlrHlrAdminGetInfo.cpp
glite_dgas_hlr_queryadmin_LDADD = $(LDADD)

glite_dgas_hlr_queryresource_SOURCES = hlrRGetInfo.cpp
glite_dgas_hlr_queryresource_LDADD = $(LDADD)

glite_dgas_hlr_deladmin_SOURCES = hlrDelHlrAdmin.cpp
glite_dgas_hlr_deladmin_LDADD = $(LDADD)

glite_dgas_hlr_delresource_SOURCES = hlrDelResource.cpp
glite_dgas_hlr_delresource_LDADD = $(LDADD)


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

