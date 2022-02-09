#!/bin/bash

basefile=LOAD_DATA_FROM_MYSQL_TO_HIVE
log_dir=logs/
currdate=`date +"%Y-%m-%d_%T"`
logfile=${log_dir}${basefile}_${currdate}.log

if [ $# -ne 2 ]
then
	echo "Missing Config File !!!!!" 
	exit 1
fi

paramfile=$1
sqoopjobfile=$2

. ${paramfile}

mysql -u root -p`cat $SQLPASSWORD_FILE` -e "
    source sqoop_scripts/db_views_queries.sql;
"
# v_salesorderheader, v_salesorderdetails, v_product, v_customer, 
if [ $? -eq 0 ]
then 
    echo "VIEWS CREATED SUCCESSFULLY"
    echo "VIEWS CREATED SUCCESSFULLY" >> ${logfile}
else
	 echo "ERROR!!! VIEWS DID NOT CREATED" 
	 echo "ERROR!!! VIEWS DID NOT CREATED"  >> ${logfile}
	 exit 1
fi



`./${sqoopjobfile} ${paramfile}`
