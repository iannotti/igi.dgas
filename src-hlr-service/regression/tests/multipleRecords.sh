if [ -n $2 ];
then
accountingProcedure="accountingProcedure=$2";
fi
TIMESTAMP=`cat $TESTBASE/timestamp.tmp`
DATE=`cat $TESTBASE/date.tmp`
i=0
start=`date +%s`
elapsed=0;
while 
        true; 
do
        (( i = $i + 1 ))
        echo -n "Iteration No:$i"
        if [ $i -eq $3 ] ;
        then
                echo -n "Exiting at iteration: $i"
		end=`date +%s`
		(( elapsed = $end - $start ))
		echo "Elaspes: $elapsed seconds"
                exit 0;
        fi
	PROGRESSIVE=$i
	JOBID="selfTestProgr$i"
	${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:" CPU_TIME=$PROGRESSIVE WALL_TIME=$PROGRESSIVE PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=$PROGRESSIVE.selftest" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" GlueCEInfoTotalCPUs="176" $accountingProcedure
done


