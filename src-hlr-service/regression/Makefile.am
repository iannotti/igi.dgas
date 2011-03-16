SUBDIRS= tests


regressiondir=$(libexecdir)/dgastests

EXTRA_DIST = \
	test.conf \
	level1.test \
	level2.test \
	full.test \
	sensors_full.test \
	sensors_4_full.test \
	storage.test
 
regression_DATA=\
	test.conf \
	level1.test \
	level2.test \
	full.test \
	sensors_full.test \
	storage.test

regression_SCRIPTS=\
	glite-dgas-testrun.pl \
	joinCover.pl 

MAINTAINERCLEANFILES = Makefile.in 

