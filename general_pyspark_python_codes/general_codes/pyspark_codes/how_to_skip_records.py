from pyspark.sql import SparkSession
from pyspark import SparkConf

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "skip_records")
spark_conf.set("master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
sc = spark.sparkContext
data_rdd = sc.textFile("../DataFiles/skip.csv")

# output of data_rdd.zipWithIndex():

# ('line1', 0)
# ('line2', 1)
# ('line3', 2)
# ('id,name,loc', 3)
# ('1,ravi,bengaluru', 4)
# ('2,raj,chennai', 5)
# ('3,prasad,pune', 6)

data_rdd_withIndex = data_rdd.zipWithIndex().filter(lambda x: x[1] > 2)
mapped_rdd_data = data_rdd_withIndex.map(lambda x: x[0].split(","))

# +---+------+---------+
# | _1|    _2|       _3|
# +---+------+---------+
# | id|  name|      loc|
# |  1|  ravi|bengaluru|
# |  2|   raj|  chennai|
# |  3|prasad|     pune|
# |  4|shekar|  chennai|
# +---+------+---------+

columns = mapped_rdd_data.collect()[0]
# skip_line = mapped_rdd_data.first()

final_data = mapped_rdd_data.filter(lambda x: x != columns).toDF(columns)

final_data.show()

# +---+------+---------+
# | id|  name|      loc|
# +---+------+---------+
# |  1|  ravi|bengaluru|
# |  2|   raj|  chennai|
# |  3|prasad|     pune|
# |  4|shekar|  chennai|
# +---+------+---------+

spark.stop()
