## Execution Flow of the Code

<ul>
    <li>
    STEP 1: Create a folder <b>`logs`</b> (to store the logs)
    </li>
    <li>
    STEP 2: Create a file named <b>`sql.pwd`</b> <br>
            storing the password of the MYSQL server inside the <b>env</b> folder             
    </li>
    <li>
    STEP 3: GIVE the execution permission to all the files present in the <b>mainscripts</b> files <br>
            <pre>
                chmod u+x mainscripts/*
            </pre>
    </li>
    <li>
    STEP 4: Copy The airflow/dags/scd_in_warehouse.py file to the respective airflow execution folder <br> Set the <b>ROOT_PATH</b>
    as per the Project location (i.e: SCD_in_Warehouse) 
    </li>
</ul>



## Commands Execution Flow Manual Execution

### Make Sure you are running the HDFS, Hive, AirFlow for the below code
``
### 1 Loading the data to MySQL server (First Time)
<pre>
    ./mainscripts/loadthedata_to_mysql.sh env/params.prm
</pre>

### 2 Loading the  MySQL data to HDFS (First Time)
<pre>
    ./mainscripts/loadthedatafrom_mysql_to_hdfs.sh env/params.prm sqoop_scripts/sqoop_job.sh
</pre>

### 3 Load the hive Tables (First Time)
<pre>
    ./mainscripts/loaddat_to_hive.sh
</pre>

### 4 Execute the SCD1 code
<pre>
    hive -f ./hive/creditcard_scd1.hql 
</pre>

### 5 Execute the SCD2 code
<pre>
    hive -f ./hive/customer_scd2.hql
</pre>

### 6 Execute the SCD4
<pre>   
    python3 spark/scd4.py
</pre>

### 7 Airflow Dag Name 
<pre>
    SCD_IN_WAREHOUSE
</pre>


## Help Code

### Delete all the hdfs data
<pre>
    hdfs dfs -rm -r /user/saif/HFS/Output/stores/
</pre>

## Delete customer_demo hdfs location
<pre>
     hdfs dfs -rm -r HFS/Output/stores/target/customer_demo/*
</pre>

### If the hdfs is in safe mode
<pre>
    hdfs dfsadmin -safemode leave
</pre>






