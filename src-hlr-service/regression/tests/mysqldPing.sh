echo $HLRD_CONF
eval MYSQLPWD=`grep hlr_sql_password $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
eval MYSQL_USER=`grep hlr_sql_user $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
mysqladmin -u$MYSQL_USER -p$MYSQLPWD status
