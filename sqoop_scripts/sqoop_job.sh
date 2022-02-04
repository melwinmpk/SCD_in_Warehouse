#!/bin/bash

if [ $# -ne 1 ]
then 
	echo "Param file Missing please or Password file missing"
	exit 1
fi

. ${1}


sqoop job --delete loadcustomers

sqoop-job --create loadcustomers -- import  \
--connect jdbc:mysql://localhost:${PORT_NO}/${DB_SQL}?useSSL=False \
--username root --password-file file:///home/saif/SCD_in_Warehouse/env/sql.pwd \
--table v_customer  --as-parquetfile \
--target-dir ${HADOOP_FILEPATH}/stores/sqoop/customers  --append \
--split-by CustomerID --num-mappers 1 --check-column ModifiedDate --incremental lastmodified \

sqoop-job --exec loadcustomers

# /user/saif/HFS/Output/stores/sqoop
# /user/saif/HFS/Output/Inc_Imports_Id

# sqoop script to create a new job to load salesorderheader data from rdbms to hdfs
# sqoop-job --create loadsalesorderheader -- import  --connect jdbc:mysql://quickstart:3306/adventureworks --username root \
# --table v_salesorderheader --as-parquetfile \
# --target-dir /user/cloudera/bigretail/output/stores/sqoop/salesorderheader  \
# --split-by SalesOrderID --num-mappers 1 --check-column SalesOrderID --incremental append \
# --password-file /user/cloudera/passwordfile


# sqoop script to create a new job to load salesorderdetails data from rdbms to hdfs
# sqoop-job --create loadsalesorderdetail -- import  --connect jdbc:mysql://quickstart:3306/adventureworks --username root \
# --table v_salesorderdetails  --as-parquetfile \
# --target-dir /user/cloudera/bigretail/output/stores/sqoop/salesorderdetails  \
# --split-by SalesOrderDetailID --num-mappers 1 --check-column SalesOrderDetailID --incremental append \
# --password-file /user/cloudera/passwordfile


# sqoop script to create a new job to load v_product data from rdbms to hdfs
# sqoop-job --create loadproducts -- import  --connect jdbc:mysql://quickstart:3306/adventureworks --username root \
# --table v_product  --as-parquetfile \
# --target-dir /user/cloudera/bigretail/output/stores/sqoop/products  \
# --split-by productId --num-mappers 1 --check-column modifieddate --incremental lastmodified \
# --password-file /user/cloudera/passwordfile

# sqoop job to load creditcard table
# sqoop job --create loadcreditcard -- import --connect jdbc:mysql://quickstart:3306/adventureworks --username root \
# --table creditcard --as-parquetfile --target-dir /user/cloudera/bigretail/output/stores/sqoop/creditcard  \ 
# --num-mappers 1 --check-column ModifiedDate --append --incremental lastmodified --password-file /user/cloudera/passwordfile 

if [ $? -eq 0 ]
then 
    echo "SQOOP JOB EXECUTED SUCCESSFULLY"
    echo "SQOOP JOB EXECUTED SUCCESSFULLY"
else
	 echo "ERROR!!! SQOOP JOB DID NOT EXECUTED " 
	 echo "ERROR!!! SQOOP JOB DID NOT EXECUTED "
	 exit 1
fi


#to execute any of the jobs 
# sqoop-job --exec loadsalesorderheader


