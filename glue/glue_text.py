import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType


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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

es_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="text",
    connection_options={"paths": ["s3://example-data/mcc2_input3/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

es_df = es_ds.toDF()

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

dyn_df = DynamicFrame.fromDF(focus_df, glueContext, "nested")
sink0 = glueContext.write_dynamic_frame.from_options(frame=dyn_df, connection_type="s3", format="json",
                                                     connection_options={
                                                         "path": "s3://example-output/mcc2/",
                                                         "partitionKeys": ["log_timestamp"]},
                                                     transformation_ctx="sink0")
job.commit()
