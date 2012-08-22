## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************


lib_LTLIBRARIES  = libdgas_asyncConsumer.la \
		libdgas_amqConsumer.la \
		libdgas_amqProducer.la
	
libdgas_asyncConsumer_la_SOURCES = \
	asyncConsumer.cpp

libdgas_amqConsumer_la_SOURCES = \
	amqConsumer.cpp
	
libdgas_amqProducer_la_SOURCES = \
	amqProducer.cpp 

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/interface-msg-common \
	$(GLITE_CFLAGS) \
	$(ACTIVEMQ_CPP_CFLAGS) \
	$(APR_CFLAGS) \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
