import sys
from pysqler import Select

query = Select()
query.select("sku", "SUM(qty)", "SUM(qty*price)")
query.from1("test_tb")
query.groupby("sku")
query_str = str(query)
print(query_str)
