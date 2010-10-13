echo $HLRD_CONF
eval LOCKFILE=`grep hlr_def_lock $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
[ -e $LOCKFILE ] || exit 11
PID=`cat $LOCKFILE`
echo $PID
echo $LISTENER_NAME
ps w -p $PID | grep $LISTENER_NAME
