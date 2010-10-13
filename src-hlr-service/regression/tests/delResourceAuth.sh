eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_USER=`grep hlr_sql_user $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "delete from jobTransSummary where thisGridId like '%selfTestResource%'";
if [ $? -ne 0 ]; then
	echo -n "Unable to delete spurius records in acctdesc";
fi
mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "DELETE FROM acctdesc WHERE ceId='testAuthOnly'";
if [ $? -ne 0 ]; then
	echo -n "Unable to delete auth entry in acctdesc";
	exit 2;
fi
