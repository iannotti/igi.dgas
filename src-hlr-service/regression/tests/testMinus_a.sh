JOBID="https://testhostname:9000/minus_a_Test"
TIMESTAMP="193705200"
DATE="193705200"
${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID:1" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=1 WALL_TIME=6 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=000001" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" -3

${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID:2" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN2" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=2 WALL_TIME=5 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=000002" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" -3


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

eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "SELECT count(*) FROM  trans_in  WHERE dgJobId like '$JOBID%'"
$DGAS_LOCATION/sbin/glite-dgas-hlr-translatedb
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -A 'count(dgJobId)' -j "$JOBID%" 
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'count(dgJobId),sum(cpuTime),sum(wallTime)' -j "$JOBID%"
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'count(dgJobId)' -j "$JOBID%" | cut -d "|" -f 3 | grep 2
if [ $? -ne 0 ] ;
then
	echo -n "The query didn't return 2 jobs!";
	exit 2;
fi
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'count(dgJobId),sum(cpuTime),sum(wallTime)' -j "$JOBID%" -a "selfTestUserDN2" 
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'count(dgJobId)' -j "$JOBID%" -a "selfTestUserDN2" | sed -e 's/\s//g' | cut -d "|" -f 3 | grep 1
if [ $? -ne 0 ] ;
then
	echo -n "The query didn't return only 1 job!";
	exit 3;
fi
