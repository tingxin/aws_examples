# This is a sample Python script.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType

folder_path = 's3://tingxin-mcc-input/operation-log-2021-11-01/'
folder_output = 's3://mcc-operation-etl-output'


@udf(returnType=StringType())
def extract_message(message_str):
    if message_str:
        t = message_str.index("url=")
        return message_str[t + 4:]
    return message_str


spark = SparkSession.builder.getOrCreate()

es_df = spark.read.format('json').option("multiLine", "false").load(folder_path)

fields_df = es_df.filter(F.col("method") == "POST")
fields_df = fields_df.filter(F.col("message") != "")

focus_df = fields_df.withColumn("fields_log_type", F.col("fields.log_type"))
focus_df = focus_df.withColumn("message", extract_message(F.col("message")))

focus_column_names = ["log_timestamp", "log_user", "method", "hostname", "@timestamp",
                      "log_url", "request_id", "fields_log_type", "message", "postData"]
focus_df = focus_df.select(*focus_column_names)

focus_df.coalesce(1).write.format('json').save(folder_output)
