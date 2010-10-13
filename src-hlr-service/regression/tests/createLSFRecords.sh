#!/bin/bash

DATE=`date +%s`
DATE1=$(($DATE+3600))
DATE2=$(($DATE1+3600))
DATE3=$(($DATE2+3600))

HOUR=`date +%k`
HOUR1=$(($HOUR+1))
HOUR2=$(($HOUR1+1))
HOUR3=$(($HOUR2+1))
TIMESTAMP1=`date +"%Y-%m-%d $HOUR1:%M:%S"`
TIMESTAMP2=`date +"%Y-%m-%d $HOUR2:%M:%S"`
TIMESTAMP3=`date +"%Y-%m-%d $HOUR3:%M:%S"`


sed -e "s/\$timestamp1/$DATE1/g;s/\$timestamp2/$DATE2/g;s/\$timestamp3/$DATE3/g" $TESTBASE/tests/UR_LRMS_lsf.records > $TESTBASE/tests/UR_LRMS_lsf.records.buff
sed -e "s/\$timestamp1/$TIMESTAMP1/;s/\$timestamp2/$TIMESTAMP2/;s/\$timestamp3/$TIMESTAMP3/" $TESTBASE/tests/grid-jobmap.template.lsf > $TESTBASE/tests/grid-jobmap.template.lsf.buff
