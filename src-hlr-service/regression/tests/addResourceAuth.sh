eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_USER=`grep hlr_sql_user $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`

mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "REPLACE INTO acctdesc VALUES(0,'a','testAuthOnly','testAuthOnly','$CERT_DN','tesGid')";
if [ $? -ne 0 ]; then
	echo -n "Unable to add auth entry in acctdesc";
	exit 2;
fi
