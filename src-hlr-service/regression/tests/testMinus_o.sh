JOBID="https://testhostname:9000/minus_o_Test"
TIMESTAMP="193705200"
DATE="193705200"
${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID:1" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=1 WALL_TIME=6 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=000001" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" -3

${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID:2" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=2 WALL_TIME=5 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=000002" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" -3

${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID:3" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:"  CPU_TIME=3 WALL_TIME=4 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=351731" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" -3

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
mysql -uroot -p$MYSQLPWD hlr  -e "SELECT count(*) FROM  trans_in  WHERE dgJobId like '$JOBID%'"
$DGAS_LOCATION/sbin/glite-dgas-hlr-translatedb
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -A 'count(dgJobId)' -j "$JOBID%" 
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -A 'count(dgJobId)' -j "$JOBID%" | sed -e 's/\s//g' | cut -d "|" -f 3 | grep 3
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'sum(cpuTime),sum(wallTime)' -j "$JOBID%" -G "dgJobId"
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'sum(cpuTime),sum(wallTime)' -j "$JOBID%" -G "dgJobId" | md5sum | grep 8c6843e6106fe40fedf1046b729b02ea 
if [ $? -ne 0 ]
then
	echo -n "wrong md5sum, output different from specification";
	exit 1;
fi
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'sum(cpuTime),sum(wallTime)' -j "$JOBID%" -G "dgJobId" -o "sum(cpuTime) DESC" 
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'sum(cpuTime),sum(wallTime)' -j "$JOBID%" -G "dgJobId" -o "sum(cpuTime) DESC" | md5sum | grep 4dfd90a0a43894a9faea5e3ea8e25db9
if [ $? -ne 0 ]
then
	echo -n "wrong md5sum, output different from specification";
	exit 1;
fi
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'sum(cpuTime),sum(wallTime)' -j "$JOBID%" -G "dgJobId" -o "sum(wallTime)" 
$DGAS_LOCATION/bin/glite-dgas-hlr-query  -H "$HOSTNAME:$HLR_PORT:" -Q resourceAggregate -A 'sum(cpuTime),sum(wallTime)' -j "$JOBID%" -G "dgJobId" -o "sum(wallTime)" | md5sum | grep 4dfd90a0a43894a9faea5e3ea8e25db9
if [ $? -ne 0 ]
then
	echo -n "wrong md5sum, output different from specification";
	exit 1;
fi
