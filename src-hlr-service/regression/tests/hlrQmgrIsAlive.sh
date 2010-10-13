echo $HLRD_CONF
eval LOCKFILE=`grep hlr_qmgr_def_lock $HLRD_CONF | grep "^#" -v | cut -d'"' -f 2`
[ -e $LOCKFILE ] || exit 11
PID=`cat $LOCKFILE`
ps w -p $PID | grep $QMGR_NAME
