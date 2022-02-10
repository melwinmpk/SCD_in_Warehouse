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

    df = spark.read.format("parquet").load("hdfs://localhost:9000/user/saif/HFS/Output/stores/stage/customers")
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

    postfinalformated_df.write.format("orc")\
            .mode("overwrite")\
            .save("hdfs://localhost:9000/user/saif/HFS/Output/stores/target/customer_demo")

    postfinalformated_df.show(1,truncate=False)