from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[*]", "wc_app")
ssc = StreamingContext(sc, 30)

sc.setLogLevel("ERROR")

#create a DStream
lines = ssc.socketTextStream("localhost",  9998)

#computation on RDD
words = lines.flatMap(lambda x : x.split())
pairs = words.map(lambda x:(x,1))
wordsCounts = pairs.reduceByKey(lambda x,y:x+y)
wordsCounts.pprint()
ssc.start()
ssc.awaitTermination()