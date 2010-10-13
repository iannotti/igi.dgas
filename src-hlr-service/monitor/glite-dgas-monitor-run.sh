#!/bin/bash

[ ! -z $GLITE_LOCATION ]  || export GLITE_LOCATION=/opt/glite/

currdate=`date`;
outdir="${GLITE_LOCATION}/var/dgas/";
plugindir="${GLITE_LOCATION}/libexec/dgasmonitor/plugins/"

for i in  $(ls $plugindir); do
$plugindir/$i &> $outdir/$i.out ;
echo "Report generated:"$currdate >> $outdir/$i.out ;
done
