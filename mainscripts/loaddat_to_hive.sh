#!/bin/bash

basefile=LOAD_DATA_TO_HIVE
log_dir=logs/
currdate=`date +"%Y-%m-%d_%T"`
logfile=${log_dir}${basefile}_${currdate}.log

#hive -e "
#source hive/store_stage.hql;
#"

hive -f ./hive/store_stage.hql

if [ $? -eq 0 ]
then
    echo "LOAD DATA TO HIVE STORE STAGE SUCCESSFULL"
    echo "LOAD DATA TO HIVE STORE STAGE SUCCESSFULL" >> ${logfile}
else
	 echo "ERROR!!! LOAD DATA TO HIVE STORE STAGE  NOT SUCCESSFULL"
	 echo "ERROR!!! LOAD DATA TO HIVE STORE STAGE  NOT SUCCESSFULL"  >> ${logfile}
	 exit 1
fi

hive -f ./hive/store.hql

if [ $? -eq 0 ]
then
    echo "LOAD DATA TO HIVE STORE  SUCCESSFULL"
    echo "LOAD DATA TO HIVE STORE  SUCCESSFULL" >> ${logfile}
else
	 echo "ERROR!!! LOAD DATA TO HIVE STORE NOT SUCCESSFULL"
	 echo "ERROR!!! LOAD DATA TO HIVE STORE NOT SUCCESSFULL"  >> ${logfile}
	 exit 1
fi
