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
def extract_message(message_str):
    if message_str:
        t = message_str.index("url=")
        return message_str[t + 4:]
    return message_str


fields_schema = StructType([
    StructField("log_type", StringType())
])
data_schema = StructType([
    StructField("method", StringType()),
    StructField("@timestamp", StringType()),
    StructField("request_id", StringType()),
    StructField("log_timestamp", StringType()),
    StructField("message", StringType()),
    StructField("postData", StringType()),
    StructField("log_user", StringType()),
    StructField("hostname", StringType()),
    StructField("log_url", StringType()),
    StructField("fields", fields_schema),
])

spark = SparkSession.builder.getOrCreate()
file_path = '/Users/fugui/Work/github.com/tingxin/mcc/resource/tt.json'
es_df = spark.read.option("multiLine", "false").text(file_path)

es_df = es_df.filter("value is not null")
es_df = es_df.withColumn("data", F.from_json(F.col("value"), data_schema))
es_df = es_df.select("data.*")


fields_df = es_df.filter("method is not null").filter(F.col("method") == "POST")
fields_df = fields_df.filter(F.col("message") != "")



focus_df = fields_df.withColumn("fields_log_type", F.col("fields.log_type"))
focus_df = focus_df.withColumn("message", extract_message(F.col("message")))

focus_column_names = ["log_timestamp", "log_user", "method", "hostname", "@timestamp",
                      "log_url", "request_id", "fields_log_type", "message", "postData"]
focus_df = focus_df.select(*focus_column_names)

focus_df.coalesce(1).write.format('json').save('output.json')
