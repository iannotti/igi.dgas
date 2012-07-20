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
EXTRA_DIST = LICENSE README

docdir = $(datadir)/doc/@PACKAGE@-@VERSION@
doc_DATA = LICENSE README

SUBDIRS = \
	src-common interface-common\
	src-hlr-clients interface-hlr-clients \
	config-sensors interface-sensors src-sensors \
	config-hlr-service src-hlr-service interface-hlr-service \
	config-producers src-producers interface-producers \
	config-consumers src-consumers interface-consumers

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

common-install-etics: common-src-install

msg-common-src: 
	cd ./src-msg-common ; $(MAKE) $(AM_MAKEFLAGS) 

msg-common-interface:
	cd ./interface-msg-common ; $(MAKE) $(AM_MAKEFLAGS)

msg-common: msg-common-src msg-common-interface

msg-common-src-install: msg-common-src
	cd ./src-msg-common ; $(MAKE) $(AM_MAKEFLAGS) install

msg-common-interface-install: msg-common-interface
	cd ./interface-msg-common ; $(MAKE) $(AM_MAKEFLAGS) install

msg-common-install: msg-common-src-install msg-common-interface-install

msg-common-install-etics: msg-common-src-install msg-common-interface-install
	
hlr-clients-src: 
	cd ./src-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-clients-interface: 
	cd ./interface-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-clients: common hlr-clients-src hlr-clients-interface

hlr-clients-src-install: 
	cd ./src-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) install 

hlr-clients-interface-install: 
	cd ./interface-hlr-clients ; $(MAKE) $(AM_MAKEFLAGS) install

hlr-clients-install: common-install\
	hlr-clients-src-install\
	hlr-clients-interface-install

hlr-clients-install-etics: \
	common \
 	hlr-clients-src-install

sensors-config:
	cd ./config-sensors ; $(MAKE) $(AM_MAKEFLAGS)

sensors-src: 
	cd ./src-sensors ; $(MAKE) $(AM_MAKEFLAGS) 

sensors-interface: 
	cd ./interface-sensors ; $(MAKE) $(AM_MAKEFLAGS) 

sensors: common sensors-src sensors-interface sensors-config

sensors-config-install:
	cd ./config-sensors ; $(MAKE) $(AM_MAKEFLAGS) install

sensors-src-install: 
	cd ./src-sensors ; $(MAKE) $(AM_MAKEFLAGS) install 

sensors-interface-install: 
	cd ./interface-sensors ; $(MAKE) $(AM_MAKEFLAGS) install

sensors-install: common-install\
	sensors-src-install\
	sensors-interface-install \
	sensors-config-install


sensors-install-etics: \
 	sensors-src-install\
	sensors-config-install

hlr-service-config:
	cd ./config-hlr-service ; $(MAKE) $(AM_MAKEFLAGS)

hlr-service-config-install:
	cd ./config-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) install

hlr-service-src: 
	cd ./src-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-service-interface: 
	cd ./interface-hlr-service ; $(MAKE) $(AM_MAKEFLAGS) 

hlr-service: common\
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
	hlr-service-config-install

hlr-service-install-etics: \
	hlr-service-src-install\
	hlr-service-config-install

producers-config:
	cd ./config-producers ; $(MAKE) $(AM_MAKEFLAGS)

producers-config-install:
	cd ./config-producers ; $(MAKE) $(AM_MAKEFLAGS) install

producers-src: 
	cd ./src-producers ; $(MAKE) $(AM_MAKEFLAGS) 

producers-interface: 
	cd ./interface-producers ; $(MAKE) $(AM_MAKEFLAGS) 

producers: common\
	msg-common \
	producers-src\
	producers-interface\
	producers-config

producers-src-install: 
	cd ./src-producers ; $(MAKE) $(AM_MAKEFLAGS) install 

producers-interface-install: 
	cd ./interface-producers ; $(MAKE) $(AM_MAKEFLAGS) install

producers-install: common-install\
	producers-src-install\
	producers-interface-install\
	producers-config-install

producers-install-etics: \
	producers-src-install\
	producers-config-install

consumers-config:
	cd ./config-consumers ; $(MAKE) $(AM_MAKEFLAGS)

consumers-config-install:
	cd ./config-consumers ; $(MAKE) $(AM_MAKEFLAGS) install

consumers-src: 
	cd ./src-consumers ; $(MAKE) $(AM_MAKEFLAGS) 

consumers-interface: 
	cd ./interface-consumers ; $(MAKE) $(AM_MAKEFLAGS) 

consumers: common\
	msg-common \
	hlr-service\
	consumers-src\
	consumers-interface\
	consumers-config

consumers-src-install: 
	cd ./src-consumers ; $(MAKE) $(AM_MAKEFLAGS) install 

consumers-interface-install: 
	cd ./interface-consumers ; $(MAKE) $(AM_MAKEFLAGS) install

consumers-install: common-install\
	hlr-service-install\
	consumers-src-install\
	consumers-interface-install\
	consumers-config-install

consumers-install-etics: \
	consumers-src-install\
	consumers-config-install


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
