from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, ArrayType, FloatType,DateType
import json

def parse_xml(data):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    result = {}
    root = ET.fromstring(data)
    for i in root:
        result[i.tag.split("}")[-1]] = i.text

    return json.dumps(result)

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName('Read & write xml')\
        .master('local[3]')\
        .getOrCreate()

    df = spark.read.format("parquet").load("hdfs://localhost:9000/user/saif/HFS/Output/stores/stage/customers").cache()
    iplookup_udf = udf(parse_xml)

    preformated_df = df.select(F.col("CustomerID"),F.col("Demographics"))
    preformated_df = preformated_df.withColumn("Demographics", iplookup_udf(F.col("Demographics")))


    postformated_df = preformated_df.select(F.col("CustomerID"),
                       F.json_tuple(F.col("Demographics"),
                    "TotalPurchaseYTD","DateFirstPurchase","BirthDate", "MaritalStatus", "YearlyIncome","Gender",
                    "TotalChildren","NumberChildrenAtHome","Education","Occupation","HomeOwnerFlag",
                    "NumberCarsOwned","CommuteDistance"),
                    F.current_date()
                   ).toDF("customerid","totalpurchaseytd","datefirstpurchase","birthdate", "maritalstatus", "yearlyincome","gender",
                    "totalchildren","numberchildrenathome","education","occupation","homeownerflag",
                    "numbercarsowned","commutedistance","create_date")

    postfinalformated_df = postformated_df.withColumn("totalpurchaseytd",F.col("totalpurchaseytd").cast(FloatType()))\
                        .withColumn("totalchildren",F.col("totalchildren").cast(IntegerType()))\
                        .withColumn("numberchildrenathome",F.col("numberchildrenathome").cast(IntegerType()))\
                        .withColumn("numbercarsowned",F.col("numbercarsowned").cast(IntegerType()))\
                        .withColumn("datefirstpurchase",F.date_format(F.substring(F.col("datefirstpurchase"),1,10),"yyyy-MM-dd"))\
                        .withColumn("birthdate",F.date_format(F.substring(F.col("birthdate"),1,10),"yyyy-MM-dd"))

    newcustomerdemo_df = postfinalformated_df

    postfinalformated_df.write.format("orc") \
        .mode("overwrite") \
        .save("hdfs://localhost:9000/user/saif/HFS/Output/stores/target/customer_demo")

    existingcustomer_df = postfinalformated_df
    # existingcustomer_df = spark.read.format("orc")\
    #                       .load("hdfs://localhost:9000/user/saif/HFS/Output/stores/target/customer_demo")

    outerJoin_df = existingcustomer_df.alias('exist').join(newcustomerdemo_df.alias('new'),
                                                           F.col("new.customerid") == F.col("exist.customerid"),
                                                           "outer") \
        .select(
        F.when(F.col("new.customerid").isNull() | F.isnan(F.col("new.customerid")),
               F.col("exist.customerid")).otherwise(F.col("new.customerid")).alias("customerid"),

        F.when(F.col("new.totalpurchaseytd").isNull() | F.isnan(F.col("new.totalpurchaseytd")),
               F.col("exist.totalpurchaseytd")).otherwise(F.col("new.totalpurchaseytd")).alias("totalpurchaseytd"),

        F.when(F.col("new.datefirstpurchase").isNull() | F.isnan(F.col("new.datefirstpurchase")),
               F.col("exist.datefirstpurchase")).otherwise(F.col("new.datefirstpurchase")).alias("datefirstpurchase"),

        F.when(F.col("new.birthdate").isNull(),
               F.col("exist.birthdate")).otherwise(F.col("new.birthdate")).alias("birthdate"),

        F.when(F.col("new.maritalstatus").isNull(),
               F.col("exist.maritalstatus")).otherwise(F.col("new.maritalstatus")).alias("maritalstatus"),

        F.when(F.col("new.yearlyincome").isNull() | F.isnan(F.col("new.yearlyincome")),
               F.col("exist.yearlyincome")).otherwise(F.col("new.yearlyincome")).alias("yearlyincome"),

        F.when(F.col("new.gender").isNull(),
               F.col("exist.gender")).otherwise(F.col("new.gender")).alias("gender"),

        F.when(F.col("new.totalchildren").isNull() | F.isnan(F.col("new.totalchildren")),
               F.col("exist.totalchildren")).otherwise(F.col("new.totalchildren")).alias("totalchildren"),

        F.when(F.col("new.numberchildrenathome").isNull() | F.isnan(F.col("new.numberchildrenathome")),
               F.col("exist.numberchildrenathome")).otherwise(F.col("new.numberchildrenathome")).alias(
            "numberchildrenathome"),

        F.when(F.col("new.education").isNull(),
               F.col("exist.education")).otherwise(F.col("new.education")).alias("education"),

        F.when(F.col("new.occupation").isNull(),
               F.col("exist.occupation")).otherwise(F.col("new.occupation")).alias("occupation"),

        F.when(F.col("new.homeownerflag").isNull(),
               F.col("exist.homeownerflag")).otherwise(F.col("new.homeownerflag")).alias("homeownerflag"),

        F.when(F.col("new.homeownerflag").isNull(),
               F.col("exist.homeownerflag")).otherwise(F.col("new.homeownerflag")).alias("homeownerflag"),

        F.when(F.col("new.numbercarsowned").isNull() | F.isnan(F.col("new.numbercarsowned")),
               F.col("exist.numbercarsowned")).otherwise(F.col("new.numbercarsowned")).alias("numbercarsowned"),

        F.when(F.col("new.create_date").isNull(),
               F.col("exist.create_date")).otherwise(F.col("new.create_date")).alias("birthdate")
    )
    innerjoin_df = existingcustomer_df.alias('exist').join(newcustomerdemo_df.alias('new'),
                                                           F.col("exist.customerid") == F.col("new.customerid"),
                                                           "inner") \
        .select(
        F.col('new.customerid'),
        F.col('new.totalpurchaseytd'),
        F.col('new.datefirstpurchase'),
        F.col('new.birthdate'),
        F.col('new.maritalstatus'),
        F.col('new.yearlyincome'),
        F.col('new.gender'),
        F.col('new.totalchildren'),
        F.col('new.numberchildrenathome'),
        F.col('new.education'),
        F.col('new.occupation'),
        F.col('new.homeownerflag'),
        F.col('new.numbercarsowned'),
        F.col('new.commutedistance'),
        F.col('new.create_date')
    )

    result_df = innerjoin_df.unionAll(outerJoin_df)

    result_df.show(10,truncate=False)
    result_df.write.format("orc") \
        .mode("overwrite") \
        .save("hdfs://localhost:9000/user/saif/HFS/Output/stores/target/customer_demo_history")
    # postfinalformated_df.write.format("orc") \
    #     .mode("overwrite") \
    #     .save("hdfs://localhost:9000/user/saif/HFS/Output/stores/target/customer_demo")
