#!/bin/bash

basefile=LOAD_DATA_TO_HIVE
log_dir=logs/
currdate=`date +"%Y-%m-%d_%T"`
logfile=${log_dir}${basefile}_${currdate}.log

hive -e "
source hive/eda.hql;
"

if [ $? -eq 0 ]
then 
    echo "LOAD THE DATA TO HIVE SUCCESSFULLY"
    echo "LOAD THE DATA TO HIVE SUCCESSFULLY" >> ${logfile}
else
	 echo "ERROR!!! LOAD THE DATA TO HIVE NOT DONE" 
	 echo "ERROR!!! LOAD THE DATA TO HIVE NOT DONE"  >> ${logfile}
	 exit 1
fi