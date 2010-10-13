eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_USER=`grep hlr_sql_user $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`

echo $1
if [ -z $1 ]; then
	echo -n "You MUST provide a template jobID!!!!"
	exit 4;
fi

mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "SELECT * FROM  trans_in  WHERE dgJobId like '$1%'" | grep $1
if [ $? -ne 0 ]; then
	echo -n "Job not found in the database";
        exit 1;
fi

mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "DELETE FROM trans_in WHERE dgJobId like '$1%'";
if [ $? -ne 0 ]; then
	echo -n "Unable to clean-up trans_in";
	exit 2;
fi
mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "DELETE FROM jobTransSummary WHERE dgJobId like '$1%'";
if [ $? -ne 0 ]; then
	echo -n "Unable to delte the job from jobTransSummary";
        exit 3;
fi
