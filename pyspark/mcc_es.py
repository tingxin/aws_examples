# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime, date
import pandas as pd

spark = SparkSession.builder.getOrCreate()

log_df = spark.read.option("multiLine", "true").json(
    '/Users/fugui/Work/github.com/tingxin/mcc/resource/elastic_post1.json')
order_items_df = spark.read.option("multiLine", "true").json(
    '/Users/fugui/Work/NWCD/mcc/data/newschema/order_items.json')
fields_df = log_df.filter("fields is not null").select("fields")

fields_df.printSchema()
focus_column_names = ["log_timestamp", "log_user", "code", "method", "service_name", "event_description", "log_url",
                      "postData"]


focus_df = fields_df
for item in focus_column_names:
    if focus_df:
        focus_df = focus_df.withColumn(item, F.col(f"fields.{item}"))

focus_df = focus_df.select(*focus_column_names)
focus_df.show()
