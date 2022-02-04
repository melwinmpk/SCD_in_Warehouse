#!/bin/bash

basefile=LOAD_DATA_TO_MYSQL
log_dir=logs/
currdate=`date +"%Y-%m-%d_%T"`
logfile=${log_dir}${basefile}_${currdate}.log

if [ $# -ne 1 ]
then
	echo "Missing Config File !!!!!" 
	exit 1
fi

echo "LOADING THE FILE"
paramfile=$1
. ${paramfile}	

echo "EXECUTING THE CODE"
mysql -u root -p`cat $SQLPASSWORD_FILE` -e "
    source dataset/databasescript/AWBackup.sql;
" 

if [ $? -eq 0 ]
then 
    echo "DATA BASE IMPORTED SUCCESSFULLY"
    echo "DATA BASE IMPORTED SUCCESSFULLY" >> ${logfile}
else
	 echo "ERROR!!! DATA BASE DID NOT IMPORTED" 
	 echo "ERROR!!! DATA BASE DID NOT IMPORTED"  >> ${logfile}
	 exit 1
fi