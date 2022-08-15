from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType

spark_conf = SparkConf()
spark_conf.set("master.app.name", "find_top_course")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Read Views.csv

views_schema = "userId int,chapterId int,dateAndTime TimeStamp"

views_df = spark.read.format("csv") \
    .schema(views_schema) \
    .option("header", False) \
    .option("path", "../../../../data_files/views.csv") \
    .load()

views_df.show()
# Read Chapters.csv

chapters_schema = "chapter_id int,course_id int"

chapters_df = spark.read.format("csv") \
    .option("header", False) \
    .schema(chapters_schema) \
    .option("path", "../../../../data_files/chapters.csv") \
    .load()

chapters_df.show()

# to find out how many Chapters are there per course :

views_df.createOrReplaceTempView("views")
chapters_df.createOrReplaceTempView("chapters")
spark.sql("select count(chapterId),course_id from views v join"
          " chapters c on c.chapter_id = v.chapterId group by course_id").show()

spark.stop()
