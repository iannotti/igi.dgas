i=0
eval period=`grep hlr_qmgr_pollPeriod $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
while 
	$DGAS_LOCATION/sbin/glite-dgas-hlr-checkqueue | grep `cat $TESTBASE/jobId.tmp`_$1;
do 
	(( i = $i + 1 ))
	echo -n "Iteration No:$i"
	if [ $i -eq 10 ] ;
	then
		echo -n "Exiting at iteration: $i"
		exit 1;
	fi
	sleep $period;
done
echo -n "Found job:`cat $TESTBASE/jobId.tmp`_$1"
exit 0
