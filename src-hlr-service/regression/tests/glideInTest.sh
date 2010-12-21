JOBID="280880.t2-ce-01.to.infn.it:1232914510"
TIMESTAMP=`cat $TESTBASE/timestamp.tmp`
DATE=`cat $TESTBASE/date.tmp`
${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:selftest-queue" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=6 WALL_TIME=646 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=351726" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" voOrigin="pool" jobName="selfTest" "accountingProcedure=outOfBand" -3
i=0
eval period=`grep hlr_qmgr_pollPeriod $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
while
	$DGAS_LOCATION/sbin/glite-dgas-hlr-checkqueue | grep $JOBID;
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
${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "GRID:$JOBID" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=6 WALL_TIME=646 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=351726" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" "accountingProcedure=grid"
i=0
eval period=`grep hlr_qmgr_pollPeriod $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
while
	$DGAS_LOCATION/sbin/glite-dgas-hlr-checkqueue | grep $JOBID;
do
	(( i = $i + 1 ))
	echo -n "Iteration No:$i"
	if [ $i -eq 10 ] ;
	then
		echo -n "Exiting at iteration: $i"
		exit 3;
	fi
	sleep $period;
done
eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "SELECT count(*) FROM  trans_in  WHERE dgJobId like '%$JOBID%'"
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "SELECT * FROM  trans_in  WHERE dgJobId like '%$JOBID%'"
$DGAS_LOCATION/sbin/glite-dgas-hlr-translatedb
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "SELECT * FROM  trans_in  WHERE dgJobId like '%$JOBID%'"
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "SELECT * FROM  jobTransSummary  WHERE dgJobId like '%$JOBID%'"
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -A 'count(dgJobId)' -j "%$JOBID%" 
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -A 'count(dgJobId)' -j "GRID:$JOBID" | sed -e 's/\s//g' | cut -d "|" -f 3 | grep 1
