#!/bin/bash

[ ! -z $DGAS_LOCATION ]  || export DGAS_LOCATION=/usr/

currdate=`date`;
outdir="${DGAS_LOCATION}/var/dgas/";
plugindir="${DGAS_LOCATION}/libexec/dgasmonitor/plugins/"

for i in  $(ls $plugindir); do
$plugindir/$i &> $outdir/$i.out ;
echo "Report generated:"$currdate >> $outdir/$i.out ;
done
