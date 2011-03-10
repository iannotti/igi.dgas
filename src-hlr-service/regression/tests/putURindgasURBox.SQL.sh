#!/bin/bash

cat ${TESTBASE}/tests/$1
query=`sed -f ${TESTBASE}/tests/$1 ${TESTBASE}/tests/test.pbs.UR.SQL`
sqlite3 ${TESTBASE}/tests/dgas.sqlite "$query" 
sqlite3 ${TESTBASE}/tests/dgas.sqlite 'select * from commands'
trigger=1

while
         [ "$trigger" -ne 0 ] ;
do
	grep $2 ${TESTBASE}/dgas_ce_pushd.log
	trigger=$?
	(( i = $i + 1 ))
        echo -n "It:$i;"
        if [ $i -eq 100 ] ;
        then
                echo -n "Exiting at iteration: $i"
                exit 1;
        fi
        sleep 1;
done

