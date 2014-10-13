#!/bin/bash

BASE_DIR=`dirname $0`

echo "[`date`] 尝试启动数据同步任务..."
if [ -x /usr/bin/flock ]; then
    EXEC="/usr/bin/flock -xw 0 $BASE_DIR/main.lock"
fi

if [ "$1" = "jpda" ] ; then
	if [ -z "$JPDA_TRANSPORT" ]; then
		JPDA_TRANSPORT="dt_socket"
	fi
	if [ -z "$JPDA_ADDRESS" ]; then
		JPDA_ADDRESS="8000"
	fi
	if [ -z "$JPDA_SUSPEND" ]; then
		JPDA_SUSPEND="y"
	fi
	if [ -z "$JPDA_OPTS" ]; then
		JPDA_OPTS="-agentlib:jdwp=transport=$JPDA_TRANSPORT,address=$JPDA_ADDRESS,server=y,suspend=$JPDA_SUSPEND"
	fi
	JAVA_OPTS="$JAVA_OPTS $JPDA_OPTS"
	shift
fi

EXEC="$EXEC /usr/bin/java \
		$JAVA_OPTS \
		-Xbootclasspath/a:common-cli-1.0.0-SNAPSHOT-jar-with-dependencies.jar:common-cli-example-1.0.0-SNAPSHOT.jar \
		-jar \
		common-cli-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

echo $EXEC
echo "[ .. ]"

$EXEC $@

if [ $? = 0 ]; then
	echo "[`date`] 执行任务完成。"
else
	echo "[`date`] 执行任务失败，或发现正在执行的任务，推迟执行本轮任务。"
fi

