from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DateType, StringType

spark_conf = SparkConf().setAppName("simple_join").setMaster("local[*]")
spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Reading orders data and providing proper data types :

orders_df = spark.read.format("csv"). \
    option("path", "../DataFiles/orders_join.csv"). \
    option("inferSchema", True).option("header", True).load()

orders_df.withColumn("order_id", orders_df["order_id"].cast(IntegerType()))
orders_df.withColumn("order_date", orders_df["order_date"].cast(DateType()))
orders_df.withColumn("order_customer_id", orders_df["order_customer_id"].cast(IntegerType()))
orders_df.withColumn("order_status", orders_df["order_status"].cast(StringType()))

orders_df.show()

# Reading customers data and providing proper data types :

customer_df = spark.read.format("csv"). \
    option("path", "../DataFiles/customers_join.csv"). \
    option("inferSchema", True).option("header", True).load()

customer_df.withColumn("customer_id", customer_df["customer_id"].cast(IntegerType()))
customer_df.withColumn("customer_fname", customer_df["customer_fname"].cast(StringType()))
customer_df.withColumn("customer_lname", customer_df["customer_lname"].cast(StringType()))
customer_df.withColumn("customer_email", customer_df["customer_email"].cast(StringType()))
customer_df.withColumn("customer_password", customer_df["customer_password"].cast(StringType()))
customer_df.withColumn("customer_street", customer_df["customer_street"].cast(StringType()))
customer_df.withColumn("customer_city", customer_df["customer_city"].cast(StringType()))
customer_df.withColumn("customer_state", customer_df["customer_state"].cast(StringType()))
customer_df.withColumn("customer_zipcode", customer_df["customer_zipcode"].cast(StringType()))

customer_df.show()

# Joining above tables on order_customer_id and customer_id
orders_df.join(customer_df, orders_df.order_customer_id == customer_df.customer_id, "inner").show

# join using SPARk SQL :

orders_df.createOrReplaceTempView("orders")
customer_df.createOrReplaceTempView("customers")
spark.sql("select * from orders o inner join customers c on o.order_id == c.customer_id").show()
spark.stop()
