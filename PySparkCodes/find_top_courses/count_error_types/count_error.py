# We have data in below format :
# level,datetime
# DEBUG,2015-2-6 16:24:07
# WARN,2016-7-26 18:54:43
# INFO,2012-10-18 14:35:19
# DEBUG,2012-4-26 14:26:50
# we need to find out how many errors of each typed happened per month.
# Expected Output :
# DEBUG,January,2123
# INFO,October,2341

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.types import StringType, DateType

spark_conf = SparkConf().setAppName("count_error").setMaster("local[*]")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

error_log_df = spark.read.format("csv").option("path", "../../data_files") \
    .option("header", True).option("mode", "DROPMALFORMED").load()

log_df = error_log_df.select("level",
                             # Extract month from DateTime :
                             date_format(error_log_df["datetime"], "MMMM").alias("month"),
                             # Extract month number from DateTime which will be used for sorting :
                             date_format(error_log_df["datetime"], "MM").alias("month_num")
                             ).dropna()

log_df.createOrReplaceTempView("log")
spark.sql("select level,"
          "count(level) as error_count,"
          "month,"
          "month_num"
          " from log group by month,level,month_num "
          "order by month_num,count(level)").drop("month_num").show()

spark.stop()