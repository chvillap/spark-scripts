from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("RDD Operations 3").setMaster("local")
sc = SparkContext(conf=conf)

# While most Spark operations work on RDDs containing any type of objects,
# a few special operations are only available on RDDs of key-value pairs.
# The most common ones are distributed “shuffle” operations, such as grouping
# or aggregating the elements by a key.

# In Python, these operations work on RDDs containing built-in Python tuples.
# Simply create such tuples and then call your desired operation.

lines = sc.textFile("LoremIpsum.txt")
pairs = lines.flatMap(lambda s: s.split()).map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b).sortBy(lambda t: -t[1])

print(counts.take(10))

sc.stop()
