# Find the running total of the invoice value

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f

spark_conf = SparkConf().setAppName("window_aggregation").setMaster("local[*]")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

invoice_df = spark.read.format("csv").option("path", "../DataFiles/windowdata.csv") \
    .option("header", True)\
    .option("inferSchema", True).load()

invoice_df.printSchema()

# define the window
window_form = Window.partitionBy("country").orderBy("weeknum") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# apply the window function
invoice_df.withColumn("RunningTotal",
                      f.sum("invoicevalue").over(window_form)
                      ).show()

spark.stop()
