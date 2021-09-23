# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime, date
import pandas as pd


@udf(returnType=StringType())
def extract_date(str_date):
    if str_date and isinstance(str_date, str):
        return str_date.split("T")[0]
    return None


spark = SparkSession.builder.getOrCreate()
order_items_df = spark.read.option("multiLine", "true").json('/Users/fugui/Work/NWCD/mcc/data/local/order_items.json')

order_items_df = order_items_df.withColumn("item", F.explode(F.col('orderItems')))
order_items_df = order_items_df.drop("orderItems", "nextToken")
order_items_df = order_items_df.withColumn("price", F.col("item.itemPrice.amount").cast("double"))
order_items_df = order_items_df.withColumn("quantity", F.col("item.quantityOrdered"))
order_items_df = order_items_df.withColumn("orderItemId", F.col("item.orderItemId"))
order_items_df = order_items_df.withColumn("SKU", F.col("item.sellerSKU"))
order_items_df = order_items_df.drop("item")
print(order_items_df.count())

order_address_df = spark.read.option("multiLine", "true").json(
    '/Users/fugui/Work/NWCD/mcc/data/local/order_address1.json')

order_address_df = order_address_df.withColumn("countryCode", F.col("shippingAddress.countryCode"))
order_address_df = order_address_df.withColumn("city", F.col("shippingAddress.city"))
order_address_df = order_address_df.drop("shippingAddress")

print(order_address_df.count())

order_buyer_info_df = spark.read.option("multiLine", "true").json(
    '/Users/fugui/Work/NWCD/mcc/data/local/order_buyer_info.json')

order_buyer_info_df.printSchema()
order_buyer_info_df = order_buyer_info_df.select("amazonOrderId", "buyerEmail")
print(order_address_df.count())

order_info_df = spark.read.option("multiLine", "true").json(
    '/Users/fugui/Work/NWCD/mcc/data/local/orders.json')

order_info_df = order_info_df.select("amazonOrderId", "lastUpdateDate", "orderStatus")

df = order_items_df.join(order_address_df, "amazonOrderId", how="left")
df = df.join(order_buyer_info_df, "amazonOrderId", how="left")
df = df.join(order_info_df, "amazonOrderId", how="left")
df = df.withColumn("order_date", extract_date(F.col("lastUpdateDate")))
df = df.select("amazonOrderId", "buyerEmail", "countryCode", "city","order_date",
               "lastUpdateDate", "orderStatus", "SKU", "orderItemId", "quantity", "price")

df.show(10)
