from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("master.app.name", "find_top_course")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Read Views.csv

views_schema = "userId int,chapterId int,dateAndTime TimeStamp"

views_df = spark.read.format("csv") \
    .schema(views_schema)\
    .option("header", False) \
    .option("path", "C:/Users/abhsingh/Downloads/views1-201108-004545.csv") \
    .load()

# Read Chapters.csv

chapters_schema = "chapter_id int,course_id int"

chapters_df = spark.read.format("csv")\
    .option("header",False)\
    .schema(chapters_schema)\
    .option("path","C:/Users/abhsingh/Downloads/chapters-201108-004545.csv")\
    .load()

# to find out how many Chapters are there per course :



spark.stop()
