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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


@udf(returnType=StringType())
def extract_date(str_date):
    if str_date and isinstance(str_date, str):
        return str_date.split("T")[0]
    return None


order_items_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[0][*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/order_items/"], "recurse": True},
    transformation_ctx="order_items_ds")

order_items_df = order_items_ds.toDF()
print("check order_items_df code: {0}".format(order_items_df.count()))
order_items_df = order_items_df.withColumn("item", F.explode(F.col('orderItems')))
order_items_df = order_items_df.drop("orderItems", "nextToken")
order_items_df = order_items_df.withColumn("price", F.col("item.itemPrice.amount").cast("double"))
order_items_df = order_items_df.withColumn("quantity", F.col("item.quantityOrdered"))
order_items_df = order_items_df.withColumn("orderItemId", F.col("item.orderItemId"))
order_items_df = order_items_df.withColumn("SKU", F.col("item.sellerSKU"))
order_items_df = order_items_df.drop("item")
print("check order_items_df code: {0}".format(order_items_df.count()))

order_address_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[0][*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/order_address/"], "recurse": True},
    transformation_ctx="order_address_ds")

order_address_df = order_address_ds.toDF()
print("check order_address_df code: {0}".format(order_address_df.count()))
order_address_df = order_address_df.withColumn("countryCode", F.col("shippingAddress.countryCode"))
order_address_df = order_address_df.withColumn("city", F.col("shippingAddress.city"))
order_address_df = order_address_df.drop("shippingAddress")
print("check order_address_df code: {0}".format(order_address_df.count()))

order_buyer_info_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[0][*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/order_buyer_info/"], "recurse": True},
    transformation_ctx="order_buyer_info_ds")

order_buyer_info_df = order_buyer_info_ds.toDF()
print("check order_buyer_info_df code: {0}".format(order_buyer_info_df.count()))
order_buyer_info_df = order_buyer_info_df.select("amazonOrderId", "buyerEmail")
print("check order_buyer_info_df code: {0}".format(order_buyer_info_df.count()))

order_info_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/orders/"], "recurse": True},
    transformation_ctx="order_info_ds")

order_info_df = order_info_ds.toDF()
print("check order_info_df code: {0}".format(order_info_df.count()))
order_info_df = order_info_df.select("amazonOrderId", "lastUpdateDate", "orderStatus")
print("check order_info_df code: {0}".format(order_info_df.count()))

df = order_items_df.join(order_address_df, "amazonOrderId", how="left")
df = df.join(order_buyer_info_df, "amazonOrderId", how="left")
df = df.join(order_info_df, "amazonOrderId", how="left")
df = df.withColumn("order_date", extract_date(F.col("lastUpdateDate")))
print("check df code: {0}".format(df.count()))

df = df.select("amazonOrderId", "buyerEmail", "countryCode", "city", "order_date",
               "lastUpdateDate", "orderStatus", "SKU", "orderItemId", "quantity", "price")

dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")
sink0 = glueContext.write_dynamic_frame.from_options(frame=dyn_df, connection_type="s3", format="parquet",
                                                     connection_options={
                                                         "path": "s3://temp-dev-test-order/log-order/",
                                                         "partitionKeys": ["order_date"]}, transformation_ctx="sink0")
job.commit()
