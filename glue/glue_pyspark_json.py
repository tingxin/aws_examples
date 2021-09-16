import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

order_items_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[0][*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/order_items/"], "recurse": True},
    transformation_ctx="order_items_ds")

order_items_df = order_items_ds.toDF()
order_items_df = order_items_df.withColumn("item", F.explode(F.col('orderItems')))
order_items_df = order_items_df.drop("orderItems", "nextToken")
order_items_df = order_items_df.withColumn("price", F.col("item.itemPrice.amount").cast("double"))
order_items_df = order_items_df.withColumn("quantity", F.col("item.quantityOrdered"))
order_items_df = order_items_df.withColumn("orderItemId", F.col("item.orderItemId"))
order_items_df = order_items_df.withColumn("SKU", F.col("item.sellerSKU"))
order_items_df = order_items_df.drop("item")

order_address_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[0][*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/order_address/"], "recurse": True},
    transformation_ctx="order_address_ds")

order_address_df = order_address_ds.toDF()
order_address_df = order_address_df.withColumn("countryCode", F.col("shippingAddress.countryCode"))
order_address_df = order_address_df.withColumn("city", F.col("shippingAddress.city"))
order_address_df = order_address_df.drop("shippingAddress")

order_buyer_info_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[0][*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/order_buyer_info/"], "recurse": True},
    transformation_ctx="order_buyer_info_ds")

order_buyer_info_df = order_buyer_info_ds.toDF()
order_buyer_info_df = order_buyer_info_df.select("amazonOrderId", "buyerEmail")

order_info_ds = glueContext.create_dynamic_frame.from_options(
    format_options={"jsonPath": "$[*]", "multiline": True},
    connection_type="s3", format="json", connection_options={
        "paths": ["s3://mcc-data-stage2/orders/"], "recurse": True},
    transformation_ctx="order_info_ds")

order_info_df = order_info_ds.toDF()
order_info_df = order_info_df.select("amazonOrderId", "lastUpdateDate", "orderStatus")

df = order_items_df.join(order_address_df, "amazonOrderId", how="left")
df = df.join(order_buyer_info_df, "amazonOrderId", how="left")
df = df.join(order_info_df, "amazonOrderId", how="left")

df = df.select("amazonOrderId", "buyerEmail", "countryCode", "city",
               "lastUpdateDate", "orderStatus", "SKU", "orderItemId", "quantity", "price")

dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")
sink0 = glueContext.write_dynamic_frame.from_options(frame=dyn_df, connection_type="s3", format="json",
                                                     connection_options={
                                                         "path": "s3://mcc-data-stage2/model_order/",
                                                         "partitionKeys": []}, transformation_ctx="sink0")
job.commit()
