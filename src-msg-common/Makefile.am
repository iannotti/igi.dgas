## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************


lib_LTLIBRARIES  = libglite_dgas_asyncConsumer.la \
		libglite_dgas_AMQConsumer.la \
		libdgas_amqProducer.la
	
libglite_dgas_asyncConsumer_la_SOURCES = \
	asyncConsumer.cpp

libglite_dgas_AMQConsumer_la_SOURCES = \
	AMQConsumer.cpp
	
libdgas_amqProducer_la_SOURCES = \
	amqProducer.cpp 

AM_CPPFLAGS = \
	-I$(top_srcdir)/interface-common \
	-I$(top_srcdir)/interface-msg-common \
	$(GLITE_CFLAGS) \
	$(ACTIVEMQ_CPP_CFLAGS) \
	-DVERSION=\"@VERSION@\"
MAINTAINERCLEANFILES = Makefile.in 
