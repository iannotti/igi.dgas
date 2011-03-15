#!/bin/bash

PROXY_TTL_HOURS=720      # 30 days
PROXY_MIN_TTL_SEC=172800 # 2 days

PREFIX=${DGAS_LOCATION}

if [ -z ${DGAS_LOCATION} ]; then
    echo "Please set the DGAS_LOCATION environment variable."
    exit 1
fi


if [ "$1" = "-h" -o "$1" = "--help" ]; then
    echo "Usage: $0 [<hlr_conf_file>]"
    exit 0
fi

CONFFILE=/etc/dgas/dgas_hlr.conf
if [ "$1" != "" ]; then
    CONFFILE=$1
fi

if [ ! -f $CONFFILE ]; then
    echo "Configuration file $CONFFILE does not exist."
    exit 1
fi


PROXY_INIT_CMD=$PREFIX/bin/voms-proxy-init


eval PROXY_FILE=`grep hostProxyFile $CONFFILE | grep "^#" -v | sed -e "s/hostProxyFile\s*=\s*\"\(.*\)\"/\1/"`
HLR_USER=`grep hlr_user $CONFFILE | grep "^#" -v | sed -e "s/hlr_user\s*=\s*\"\(.*\)\"/\1/"`

# first check the validity (and existence) of the proxy for at least
# PROXY_MIN_TTL_SEC seconds:

eval 'openssl x509 -in $PROXY_FILE -checkend $PROXY_MIN_TTL_SEC &> /dev/null'

# if not valid for at least 2 days or not existent:
if [ "$?" -ne "0" ]; then

    # try to generate a new proxy for PROXY_TTL_HOURS hours:
    eval '$PROXY_INIT_CMD -out $PROXY_FILE -valid $PROXY_TTL_HOURS:00 &> /dev/null'
    if [ "$?" -eq "0" ]; then
	chmod 600 $PROXY_FILE
	chown $HLR_USER:$HLR_USER $PROXY_FILE
    fi
fi
