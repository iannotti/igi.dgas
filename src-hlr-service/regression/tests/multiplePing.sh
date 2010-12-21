i=0
start=`date +%s`
elapsed=0;
while 
        true; 
do
        (( i = $i + 1 ))
        echo -n "Iteration No:$i"
        if [ $i -eq $1 ] ;
        then
                echo -n "Exiting at iteration: $i"
		end=`date +%s`
		(( elapsed = $end - $start ))
		echo "Elapsed: $elapsed seconds"
                exit 0;
        fi
	${DGAS_LOCATION}/bin/glite-dgas-ping -s "$HOSTNAME:$HLR_PORT:" -t 1 
done


