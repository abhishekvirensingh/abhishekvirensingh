# This demo will include how to perform simple aggregations in PySpark.
# We will be loading the data file order_data.csv and will calculate :
# total_number_of_rows, total_quantity,avg_unit_price,number_of_unique_invoices
# we will perform simple aggregations using :
# 1> Column Object Expression
# 2> String Expressions
# 3> Spark SQL

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf = SparkConf().setAppName("simple_aggregate").setMaster("local[*]")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

order_df = spark.read.format("csv") \
    .option("path", "../DataFiles/order_data.csv") \
    .option("inferSchema", True) \
    .option("header", True).load()

# order_df.printSchema()
# order_df.show()
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country|
# +---------+---------+--------------------+--------+---------------+---------+----------+--------------+
# |   536378|     null|PACK OF 60 DINOSA...|      24|01-12-2010 9.37|     0.55|     14688|United Kingdom|
# |   536378|     null|PACK OF 60 PINK P...|      24|01-12-2010 9.37|     0.55|     14688|United Kingdom|

# total_number_of_rows, total_quantity,avg_unit_price,number_of_unique_invoices using
# Column Object Expression

order_df.select(
    count("*").alias("total_number_of_rows"),
    sum("Quantity").alias("total_quantity"),
    avg("UnitPrice").alias("avg_unit_price"),
    countDistinct("InvoiceNo").alias("number_of_unique_invoices")
).show()

# total_number_of_rows, total_quantity,avg_unit_price,number_of_unique_invoices using
# String Expression

order_df.selectExpr("count(*) as total_number_of_rows", "sum(Quantity) as total_quantity",
                    "avg(UnitPrice) as avg_unit_price",
                    "count(distinct(InvoiceNo)) as number_of_unique_invoices").show()

# total_number_of_rows, total_quantity,avg_unit_price,number_of_unique_invoices using
# spark sql

order_df.createOrReplaceTempView("orders")
spark.sql("select count(*) ,sum(Quantity) , avg(UnitPrice) , count(distinct(InvoiceNo)) from orders") \
    .toDF("total_number_of_rows", "total_quantity", "avg_unit_price", "number_of_unique_invoices").show()

spark.stop()
