echo $HLRD_CONF
eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_HLR=`grep hlr_tmp_sql_dbname $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_USER=`grep hlr_sql_user $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
mysql -u$MYSQL_USER -p$MYSQLPWD $MYSQL_HLR -e "show tables"
