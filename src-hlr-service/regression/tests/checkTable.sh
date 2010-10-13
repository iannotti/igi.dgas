eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep $1 $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_USER=`grep hlr_sql_user $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`

mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "DESCRIBE $2" | md5sum | grep $3
if [ $? -ne 0 ]; then
	echo -n "Wrong Schema: ";
	mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR  -e "DESCRIBE $2"
        exit 1;
fi

