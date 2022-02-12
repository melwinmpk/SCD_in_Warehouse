# SCD_in_Warehouse

### Understand the various types of SCDs and implement these slowly changing dimesnsion in Hadoop Hive and Spark.

### Complete Overview
<img src="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/img/CompleteFlow.PNG?raw=true"></img>

### Execution Flow
<a href="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/Documentation/Execution%20Flow.md">Link</a>

### SCD 1 
<p>This method overwrites old with new data, and therefore does not track historical data. <p>

<img src="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/img/SCD1.png?raw=true"></img>

### SCD 2
<p>This method tracks historical data by creating multiple records for a given natural key in the dimensional tables with separate surrogate keys and/or different version numbers. Unlimited history is preserved for each insert.<br>
In this Project we have used <b>flag</b> method</p>

<img src="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/img/SCD2.PNG?raw=true"></img>
<ol>
  <li>Copy all new record from the source which is not present in the target, copy all updated records from the source to the temp table, copy all not updated records from source to temp ( set all the flag  as true)​</li>
<li>Copy all records  from target (which are updated in the  source record) set flag as false, Copy all the record which is not present in the source-target set the flag as true​</li>
<li>Finally after step 1 & 2 override the customer_temp to the store.customer(target)</li>
</ol>

### SCD 4
<p>SCD type 4 provides a solution to handle the rapid changes in the dimension tables. The concept lies in creating a junk dimension or a small dimension table with all the possible values of the rapid growing attributes of the dimension.</p>
<p>The Type 4 method is usually referred to as using "history tables", where one table keeps the current data, and an additional table is used to keep a record of some or all changes. Both the surrogate keys are referenced in the fact table to enhance query performance.</p>
<a href="https://www.folkstalk.com/2013/05/scd-type-4-rapid-growing-dimension.html">Reference Link1</a><br>
<a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension">Reference Link2</a><br><br>

<img src="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/img/SCD4.PNG?raw=true"></img>

### Manual Triggering 
<a href="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/Documentation/Execution%20Flow.md#commands-execution-flow-manual-execution">Link</a>

### Airflow Output
<img src="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/img/Airflow.PNG?raw=true"></img>
<img src="https://github.com/melwinmpk/SCD_in_Warehouse/blob/melwin_doc/img/Airflow2.PNG?raw=true"></img>

