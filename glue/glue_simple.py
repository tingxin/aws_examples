import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType


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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

es_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc2/es_input/"], "recurse": True},
    transformation_ctx="mcc2_ds")

es_df = es_ds.toDF()

es_df.show()

dyn_df = DynamicFrame.fromDF(es_df, glueContext, "nested")
sink0 = glueContext.write_dynamic_frame.from_options(frame=dyn_df, connection_type="s3", format="json",
                                                     connection_options={
                                                         "path": "s3://mcc2/es_output/",
                                                         "partitionKeys": ["log_timestamp"]}, transformation_ctx="sink0")
job.commit()
