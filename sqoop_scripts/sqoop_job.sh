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
--target-dir ${HADOOP_FILEPATH}/stores/stage/customers  --append \
--split-by CustomerID --num-mappers 1 --check-column ModifiedDate --incremental lastmodified \

sqoop-job --exec loadcustomers

# sqoop script to create a new job to load salesorderheader data from rdbms to hdfs
sqoop job --delete loadsalesorderheader

sqoop-job --create loadsalesorderheader -- import  \
--connect jdbc:mysql://localhost:${PORT_NO}/${DB_SQL}?useSSL=False \
--username root --password-file file:///home/saif/SCD_in_Warehouse/env/sql.pwd \
--table v_salesorderheader --as-parquetfile \
--target-dir ${HADOOP_FILEPATH}/stores/stage/salesorderheader  \
--split-by SalesOrderID --num-mappers 1 --check-column SalesOrderID --incremental append 

sqoop-job --exec loadsalesorderheader


# sqoop script to create a new job to load salesorderdetails data from rdbms to hdfs

sqoop job --delete loadsalesorderdetail

sqoop-job --create loadsalesorderdetail -- import  \
--connect jdbc:mysql://localhost:${PORT_NO}/${DB_SQL}?useSSL=False \
--username root --password-file file:///home/saif/SCD_in_Warehouse/env/sql.pwd \
--table v_salesorderdetails  --as-parquetfile \
--target-dir ${HADOOP_FILEPATH}/stores/stage/salesorderdetails  \
--split-by SalesOrderDetailID --num-mappers 1 --check-column SalesOrderDetailID --incremental append 

sqoop-job --exec loadsalesorderheader


# sqoop script to create a new job to load v_product data from rdbms to hdfs
sqoop job --delete loadproducts

sqoop-job --create loadproducts -- import  \
--connect jdbc:mysql://localhost:${PORT_NO}/${DB_SQL}?useSSL=False \
--username root --password-file file:///home/saif/SCD_in_Warehouse/env/sql.pwd \
--table v_product  --as-parquetfile \
--target-dir ${HADOOP_FILEPATH}/stores/stage/products  \
--split-by productId --num-mappers 1 --check-column modifieddate --incremental lastmodified 

sqoop-job --exec loadproducts

# sqoop job to load creditcard table
sqoop job --delete loadcreditcard 

sqoop job --create loadcreditcard -- import \
--connect jdbc:mysql://localhost:${PORT_NO}/${DB_SQL}?useSSL=False \
--username root --password-file file:///home/saif/SCD_in_Warehouse/env/sql.pwd \
--table creditcard --as-parquetfile \
--target-dir ${HADOOP_FILEPATH}/stores/stage/creditcard --num-mappers 1 --check-column ModifiedDate \
--append --incremental lastmodified 

sqoop-job --exec loadcreditcard
