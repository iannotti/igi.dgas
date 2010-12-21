#Usage Record with cpuTime=-2 and start ==0
JOBID="https://$HOSTNAME:9000/negativewct"
TIMESTAMP=`cat $TESTBASE/timestamp.tmp`
DATE=`cat $TESTBASE/date.tmp`
${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:" CPU_TIME=10 WALL_TIME=-80000 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=01010_$2.selftest" "URCREATION=$DATE" "start=$TIMESTAMP" "end=$TIMESTAMP" "ctime=167785200" "qtime=0" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" fqan="/fqanVO/Role=NULL/Group=NULL" -3
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
$DGAS_LOCATION/sbin/glite-dgas-hlr-translatedb -D
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -j "%negativewct%"
$DGAS_LOCATION/bin/glite-dgas-hlr-query -Q resourceAggregate -j "%negativewct%" | sed -e 's/\s//g' | cut -d "|" -f 5 | grep "0.00"

