#!/bin/bash

MONGO_PATH=$MONGO_PATH

if [ -x $MONGO_PATH ]; then
	EXEC_MONGO="$MONGO_PATH $1 --shell functions.js"
else
	EXEC_MONGO="mongo $1 --shell functions.js"
fi

echo $EXEC_MONGO
$EXEC_MONGO
