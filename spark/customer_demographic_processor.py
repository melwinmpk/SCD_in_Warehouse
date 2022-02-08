from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import xml.etree.ElementTree as ET
from pyspark.sql.functions import udf
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
    spark = SparkSession.builder \
        .appName('Read & write xml') \
        .master('local[3]') \
        .config('spark.jars', '/home/saif/LFS/jars/spark-xml_2.12-0.5.0.jar') \
        .getOrCreate()

    df = spark.read.format("parquet").load("hdfs://localhost:9000/user/saif/HFS/Output/stores/sqoop/customers")
    iplookup_udf = udf(parse_xml)

    formated_df = df.select(F.col("CustomerID"), F.col("Demographics"))
    formated_df = formated_df.withColumn("Demographics", iplookup_udf(F.col("Demographics")))

    formated_df1 = formated_df.select(F.col("CustomerID"),
                                      F.json_tuple(F.col("Demographics"),
                                                   "TotalPurchaseYTD", "DateFirstPurchase", "BirthDate",
                                                   "MaritalStatus", "YearlyIncome", "Gender",
                                                   "TotalChildren", "NumberChildrenAtHome", "Education", "Occupation",
                                                   "HomeOwnerFlag",
                                                   "NumberCarsOwned", "CommuteDistance")
                                      ).toDF("CustomerID", "TotalPurchaseYTD", "DateFirstPurchase", "BirthDate",
                                             "MaritalStatus", "YearlyIncome", "Gender",
                                             "TotalChildren", "NumberChildrenAtHome", "Education", "Occupation",
                                             "HomeOwnerFlag",
                                             "NumberCarsOwned", "CommuteDistance")

    formated_df1.write.format("parquet") \
        .mode("append") \
        .save("hdfs://localhost:9000/user/saif/HFS/Output/stores/spark/customer_demographics")

    formated_df1.show(1)




