## Execution Flow of the Code

<ul>
    <li>
    STEP 1: Creat a folder <b>`logs`</b> (to store the logs)
    </li>
    <li>
    STEP 2: Creat a file named <b>`sql.pwd`</b> <br>
            storing the password of the MYSQL server inside <b>env</b> folder             
    </li>
    <li>
    STEP 3: GIVE the execution permission to all the files present in the mainscripts file <br>
            <pre>
                chmod u+x mainscripts/*
            </pre>
    </li>
</ul>

## Commands Execution Flow

### 1 Loading the data to MySQL server
<pre>
    ./mainscripts/loadthedata_to_mysql.sh env/params.prm
</pre>

### 2 Loading the  MySQL data to HDFS 
<pre>
    ./mainscripts/loadthedatafrom_mysql_to_hdfs.sh env/params.prm sqoop_scripts/sqoop_job.sh
</pre>

### 3 Load the hive Tables
<pre>
    ./mainscripts/loaddat_to_hive.sh
</pre>

### 4 Execute the SCD1 code
<pre>
    hive -f ./hive/creditcard_scd1.hql 
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






