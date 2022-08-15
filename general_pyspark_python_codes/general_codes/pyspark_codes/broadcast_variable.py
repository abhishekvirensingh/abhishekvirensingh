########################################################################
# This is a simple demonstration of "how the broadcast variable works in Spark"
# We are creating a broadcast variable with the words present in the file boring words
# and we want to exclude these words while we process our big_data_campaign_data file
########################################################################

from pyspark import SparkContext


# creating a set as duplicate are not needed in the broadcast variable:

def convert_to_set():
    boring_set = set()
    file_source = open("../DataFiles/boring_words.txt", "r")
    file_content = file_source.read().splitlines()
    for each_word in file_content:
        boring_set.add(each_word)
    return boring_set


sc = SparkContext("local[*]", "broadcast")
broad_set = sc.broadcast(convert_to_set())  # broadcast the set to each spark driver

data = sc.textFile("../DataFiles/big_data_campaign_data.csv")

required_content = data.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))
final_tuple = required_content.flatMapValues(lambda x: x.split(" ")).map(lambda x: (x[1].lower(), x[0]))
filtered_data = final_tuple.filter(lambda x: x[0] not in broad_set.value)  # filter out the data using broadcast set
output = filtered_data.reduceByKey(lambda x, y: (x + y))
sorted_output = output.sortBy(lambda x: x[1])
final_data = sorted_output.collect()
for aline in final_data:
    print(aline)
