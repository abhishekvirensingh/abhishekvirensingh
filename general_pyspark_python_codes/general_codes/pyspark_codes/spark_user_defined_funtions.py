############################################################################
# There are two ways to create UDF in spark :
# 1> Using Column Object Expression (not registered in catalog)
# 2> Using SQL Expressions (registered in catalog)
##############################################################################
# Lets consider the below data :
# Abhishek,30,Bengaluru
# Akanksha,27,Pune
# Ayantika,2,Pune
# Swati,32,Bengaluru
# Kush,3,Bengaluru
# we need to add a 4th column where if age > 18 we need to populate Y else N

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

data = [["Abhishek", 30, "Bengaluru"],
        ["Akanksha", 27, "Pune"],
        ["Ayantika", 2, "Pune"],
        ["Swati", 32, "Bengaluru"],
        ["Kush", 3, "Bengaluru"]]

spark_conf = SparkConf().setAppName("UDF").setMaster("local[*]")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()


def age_check(age):
    if age > 18:
        return "y"
    else:
        return "N"


data_df = spark.createDataFrame(data, ("name", "age", "city"))

# Using Column Object Expression:

check_age_func = udf(age_check, StringType())  # define udf
data_df.withColumn("isAdult", check_age_func(data_df.age)).show()

# using SQL expression :

spark.udf.register("check_age_function", age_check, StringType())

# for a_function in spark.catalog.listFunctions():
#     print(a_function)

data_df.withColumn("isAdult", expr("check_age_function(age)")).show()

spark.stop()
