# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType
from datetime import datetime, date
import pandas as pd
import json


@udf(returnType=StringType())
def extract_array(input_array):
    if input_array and isinstance(input_array, list) and len(input_array) == 1:
        return input_array[0]
    return input_array


post_data_destinationAddress_schema = StructType([
    StructField("name", StringType()),
    StructField("line1", StringType()),
    StructField("line1", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("countryCode", StringType()),
    StructField("postalCode", StringType())]

)
post_data_schema = StructType([
    StructField("destinationAddress", StructType()),
    StructField("items", ArrayType(StructType())),
    StructField("shouldIncludeCOD", BooleanType()),
    StructField("shouldIncludeDeliveryWindows", BooleanType()),
    StructField("isBlankBoxRequired", BooleanType()),
    StructField("blockAMZL", BooleanType())
])

spark = SparkSession.builder.getOrCreate()
file_path = '/Users/fugui/Work/github.com/tingxin/mcc/resource/elastic_post1.json'
log_df = spark.read.option("multiLine", "true").json(file_path)

fields_df = log_df.filter("fields is not null").select("fields.*")
fields_df = fields_df.toDF(*(c.replace('.', '_') for c in fields_df.columns))
fields_df.printSchema()
focus_column_names = ["log_timestamp", "log_user", "code", "message", "method", "service_name", "event_description",
                      "log_url",
                      "postData"]

focus_df = fields_df
for item in focus_column_names:
    if focus_df:
        focus_df = focus_df.withColumn(item, extract_array(F.col(f"{item}")))

focus_df = focus_df.select(*focus_column_names)
focus_df.show()
focus_df.printSchema()
# focus_df = focus_df.withColumn("postData", F.from_json(F.col("postData"), post_data_schema))
postData = focus_df.select("postData")
postData.show()
postData.printSchema()
