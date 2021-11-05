# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType
from datetime import datetime, date
import pandas as pd
import json


@udf(returnType=StringType())
def extract_array(input_array):
    if input_array and isinstance(input_array, list) and len(input_array) == 1:
        return input_array[0]
    return input_array


@udf(returnType=StringType())
def extract_message(message_str):
    if message_str:
        t = message_str.index("url=")
        return message_str[t + 4:]
    return message_str


post_data_destinationAddress_schema = StructType([
    StructField("name", StringType()),
    StructField("line1", StringType()),
    StructField("line1", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("countryCode", StringType()),
    StructField("postalCode", StringType())]

)

items_schema = StructType([
    StructField("merchantSku", StringType()),
    StructField("asin", StringType()),
    StructField("fnSku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("referenceItemId", StringType()),
])
post_data_schema = StructType([
    StructField("destinationAddress", post_data_destinationAddress_schema),
    StructField("items", ArrayType(items_schema)),
    StructField("shouldIncludeCOD", BooleanType()),
    StructField("shouldIncludeDeliveryWindows", BooleanType()),
    StructField("isBlankBoxRequired", BooleanType()),
    StructField("blockAMZL", BooleanType())
])

spark = SparkSession.builder.getOrCreate()
file_path = '/resource/data/tt.json'
log_df = spark.read.option("multiLine", "true").json(file_path)

fields_df = log_df.filter("fields is not null").select("fields.*")
fields_df = fields_df.toDF(*(c.replace('.', '_') for c in fields_df.columns))


focus_column_names = ["log_timestamp", "log_user", "fields_log_type", "method", "hostname", "@timestamp",
                      "log_url", "request_id", "message", "postData"]

focus_df = fields_df
focus_df.printSchema()
for item in focus_column_names:
    if focus_df:
        focus_df = focus_df.withColumn(item, extract_array(focus_df[item]))

focus_df = focus_df.withColumn("message", extract_message(F.col("message")))

focus_df = focus_df.select(*focus_column_names)

# focus_api = ["https://sellercentral.amazon.com/mcf/api/SearchListings"]
# focus_df = focus_df.withColumn("postData", F.from_json(F.col("postData"), post_data_schema))
focus_df.coalesce(1).write.format('json').save('output.json')

