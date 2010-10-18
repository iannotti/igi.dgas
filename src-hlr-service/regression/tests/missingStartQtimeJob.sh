#Usage Record with cpuTime=-2 and start ==0
JOBID="https://$HOSTNAME:9000/missingstartqtime"
TIMESTAMP=`cat $TESTBASE/timestamp.tmp`
DATE=`cat $TESTBASE/date.tmp`
${GLITE_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:" CPU_TIME=0 WALL_TIME=0 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=01010_$2.selftest" "URCREATION=$DATE" "start=0" "end=$TIMESTAMP" "ctime=167785200" "qtime=0" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" fqan="/fqanVO/Role=NULL/Group=NULL" -3
i=0
eval period=`grep hlr_qmgr_pollPeriod $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
while
        $GLITE_LOCATION/sbin/glite-dgas-hlr-checkqueue | grep $JOBID;
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
$GLITE_LOCATION/sbin/glite-dgas-hlr-translatedb -D
$GLITE_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -j "%start%"
$GLITE_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -j "%start%" | sed -e 's/\s//g' | cut -d "|" -f 1 | grep "1975-04-27"
