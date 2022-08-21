from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[*]", "app_stateful")
ssc = StreamingContext(sc, 30)
sc.setLogLevel("ERROR")
# create a DStream:
lines = ssc.socketTextStream("localhost", 9998)
ssc.checkpoint(".")


# function to update the state:

def update_state_function(new_values, previous_state):
    if previous_state is None:
        previous_state = 0
    return sum(new_values, previous_state)


# computation on RDD :

words = lines.flatMap(lambda x: x.split())
pairs = words.map(lambda x: (x, 1))
wordsCounts = pairs.updateStateByKey(update_state_function)
wordsCounts.pprint()
ssc.start()
ssc.awaitTermination()
