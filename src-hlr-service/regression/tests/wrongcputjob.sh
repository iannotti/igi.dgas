#Usage Record with cpuTime=-2 and start ==0
if [ -n $3 ];
then
accountingProcedure="accountingProcedure=$3";
fi
JOBID="$1_$2_wrongCpu"
TIMESTAMP=`cat $TESTBASE/timestamp.tmp`
DATE=`cat $TESTBASE/date.tmp`
${DGAS_LOCATION}/libexec/glite_dgas_atmClient -j "$JOBID" -t "$TIMESTAMP" -p "PA_LOCATION" -l "$HOSTNAME:$HLR_PORT:" -C "selfTestResource.self.test.domain:2119/jobmanager-selftest-queue" -U "selfTestUserDN" -r "$HOSTNAME:$HLR_PORT:" -$2 CPU_TIME=-2 WALL_TIME=2 PMEM=3k VMEM=4k "QUEUE=queue" "USER=selftest" "LRMSID=$TIMESTAMP-$2.selftest" "URCREATION=$DATE" "start=0" "end=$TIMESTAMP" "ctime=$TIMESTAMP" "qtime=$TIMESTAMP" "etime=$TIMESTAMP" "exitStatus=0" "si2k=1" "sf2k=1" "tz=+0200" userVo="selfTest" jobName="selfTest" $accountingProcedure
