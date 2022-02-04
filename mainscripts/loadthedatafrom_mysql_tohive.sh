#!/bin/bash

basefile=LOAD_DATA_FROM_MYSQL_TO_HIVE
log_dir=logs/
currdate=`date +"%Y-%m-%d_%T"`
logfile=${log_dir}${basefile}_${currdate}.log

