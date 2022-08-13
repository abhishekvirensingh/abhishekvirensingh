# Group the data based on country and invoice number
# Total quantity based on Invoice Number Groups
# Sum of Invoice value (sum (quantity * unit price ) for each group)

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark_conf = SparkConf().setAppName("grouping_aggregations").setMaster("local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

orders_df = spark.read.format("csv").option("path", "../DataFiles/order_data.csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load()

# using SPARK SQL

orders_df.createOrReplaceTempView("orders")
spark.sql("select Country,InvoiceNo,sum(Quantity*UnitPrice) from orders group by Country,InvoiceNo").show()

# Using column object expression

orders_df.groupby("Country", "InvoiceNo").agg(expr("sum(Quantity * UnitPrice)")).show()

spark.stop()
