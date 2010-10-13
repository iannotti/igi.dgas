eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "SELECT * FROM  trans_in  WHERE dgJobId like '$1%'" | grep $1
if [ $? -ne 0 ]; then
	echo -n "Job not found in the database";
        exit 1;
fi

mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "DELETE FROM trans_in WHERE dgJobId like '$1%'";
if [ $? -ne 0 ]; then
	echo -n "Unable to delte the job from jobTransSummary";
        exit 3;
fi
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "DELETE FROM jobTransSummary WHERE dgJobId like '$1%'";
if [ $? -ne 0 ]; then
	echo -n "Unable to delte the job from jobTransSummary";
        exit 3;
fi
mysql -uroot -p$MYSQLPWD $MYSQL_HLR  -e "DELETE FROM transInLog WHERE dgJobId like '$1%'";
if [ $? -ne 0 ]; then
	echo -n "Unable to delte the job from jobTransSummary";
        exit 3;
fi
