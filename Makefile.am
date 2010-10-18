#
#       Copyright (c) 2004 on behalf of the EU EGEE Project:
#       The European Organization for Nuclear Research (CERN),
#       Istituto Nazionale di Fisica Nucleare (INFN), Italy
#       Datamat Spa, Italy
#       Centre National de la Recherche Scientifique (CNRS), France
#       CS Systeme d'Information (CSSI), France
#       Royal Institute of Technology, Center for Parallel Computers (KTH-PDC), Sweden
#       Universiteit van Amsterdam (UvA), Netherlands
#       University of Helsinki (UH.HIP), Finland
#       University of Bergen (UiB), Norway
## *********************************************************************
## *
## * Copyright (c) 2002 CERN and INFN on behalf of the EU DataGrid.
## * For license conditions see LICENSE file or
## * http://www.edg.org/license.html
## *
## *********************************************************************

## Process this file with automake to produce Makefile.in
EXTRA_DIST = LICENSE

docdir = $(datadir)/doc/@PACKAGE@-@VERSION@
doc_DATA = LICENSE

## Subdirectories list
SUBDIRS = \
	src-common interface-common\
	config-hlr-clients src-hlr-clients interface-hlr-clients \
	src-pa-clients interface-pa-clients \
	config-hlr-service src-hlr-service interface-hlr-service

## Default flags to run aclocal
ACLOCAL_AMFLAGS = -I project

common-src: 
	cd ./src-common ; $(MAKE) $(AM_MAKEFLAGS) 

common-interface: 
	cd ./interface-common ; $(MAKE) $(AM_MAKEFLAGS) 

common: common-src common-interface


common-src-install: common-src
	cd ./src-common ; $(MAKE) $(AM_MAKEFLAGS) install

common-interface-install: common-interface
	cd ./interface-common ; $(MAKE) $(AM_MAKEFLAGS) install

common-install: common-src-install common-interface-install

common-install-etics: common-src-install common-interface-install

hlr-clients-config:
	cd ./config-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS)

hlr-clients-src: 
	cd ./src-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-clients-interface: 
	cd ./interface-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-clients: common hlr-clients-src hlr-clients-interface hlr-clients-config

hlr-clients-config-install:
	cd ./config-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) install

hlr-clients-src-install: 
	cd ./src-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) install 

hlr-clients-interface-install: 
	cd ./interface-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) install

hlr-clients-install: common-install\
	hlr-clients-src-install\
	hlr-clients-interface-install\
	hlr-clients-config-install

hlr-clients-install-etics: \
 	hlr-clients-src-install\
	hlr-clients-interface-install\
	hlr-clients-config-install

pa-clients-src: 
	cd ./src-pa-clients ; $(MAKE) $(AM_MAKEFLAGS) 

pa-clients-interface: 
	cd ./interface-pa-clients ; $(MAKE) $(AM_MAKEFLAGS) 

pa-clients: common pa-clients-src pa-clients-interface

pa-clients-src-install: 
	cd ./src-pa-clients ; $(MAKE) $(AM_MAKEFLAGS) install 

pa-clients-interface-install: 
	cd ./interface-pa-clients ; $(MAKE) $(AM_MAKEFLAGS) install

pa-clients-install: common-install pa-clients-src-install pa-clients-interface-install

pa-clients-install-etics: pa-clients-src-install pa-clients-interface-install


hlr-service-config:
	cd ./config-hlr-service ; $(MAKE) $(AM_MAKEFLAGS)

hlr-service-config-install:
	cd ./config-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) install

hlr-service-src: 
	cd ./src-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-service-interface: 
	cd ./interface-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-service: common\
	pa-clients\
	hlr-service-src\
	hlr-service-interface\
	hlr-service-config

hlr-service-src-install: 
	cd ./src-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) install 

hlr-service-interface-install: 
	cd ./interface-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) install

hlr-service-install: common-install\
	pa-clients-install\
	hlr-service-src-install\
	hlr-service-interface-install\
	hlr-service-config-install

hlr-service-install-etics: \
	hlr-service-src-install\
	hlr-service-interface-install\
	hlr-service-config-install


stage:
	@set fnord $(MAKEFLAGS); amf=$$2; \
	dot_seen=no; \
        target="install"; \
	prefix_arg=@prefix@; \
        list='$(SUBDIRS)'; for subdir in $$list; do \
          echo "Making $$target $$prefix_arg in $$subdir"; \
          if test "$$subdir" = "."; then \
            dot_seen=yes; \
            local_target="$$target-am"; \
          else \
            local_target="$$target"; \
          fi; \
          (cd $$subdir && $(MAKE) $(AM_MAKEFLAGS) $$local_target $$prefix_arg) \
           || case "$$amf" in *=*) exit 1;; *k*) fail=yes;; *) exit 1;; esac; \
        done; \
        if test "$$dot_seen" = "no"; then \
          $(MAKE) $(AM_MAKEFLAGS) "$$target-am" $$prefix_arg || exit 1; \
        fi; test -z "$$fail"

distsrc: dist
	mv $(distdir).tar.gz $(DISTTAR)/$(distdir)_src.tar.gz

distbin:
	@set fnord $(MAKEFLAGS); amf=$$2; \
        dot_seen=no; \
        target="install"; \
	tmpdistbin="$(WORKDIR)/dist_bin"; \
        prefix_arg="prefix=$$tmpdistbin"; \
        list='$(SUBDIRS)'; for subdir in $$list; do \
          echo "Making $$target $$prefix_arg in $$subdir"; \
          if test "$$subdir" = "."; then \
            dot_seen=yes; \
            local_target="$$target-am"; \
          else \
            local_target="$$target"; \
          fi; \
          (cd $$subdir && $(MAKE) $(AM_MAKEFLAGS) $$local_target $$prefix_arg) \
           || case "$$amf" in *=*) exit 1;; *k*) fail=yes;; *) exit 1;; esac; \
        done; \
        if test "$$dot_seen" = "no"; then \
          $(MAKE) $(AM_MAKEFLAGS) "$$target-am" $$prefix_arg || exit 1; \
        fi; test -z "$$fail"; \
	pushd $$tmpdistbin; \
	$(AMTAR) -zcf $(DISTTAR)/$(distdir)_bin.tar.gz .; \
	popd; \
	rm -rf $$tmpdistbin
