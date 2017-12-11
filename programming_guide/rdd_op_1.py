from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("RDD Operations 1").setMaster("local")
sc = SparkContext(conf=conf)

# RDDs support two types of operations: transformations, which create a new
# dataset from an existing one, and actions, which return a value to the driver
# program after running a computation on the dataset.

# All transformations in Spark are lazy, in that they do not compute their
# results right away. Instead, they just remember the transformations applied
# to some base dataset (e.g. a file). The transformations are only computed
# when an action requires a result to be returned to the driver program.

lines = sc.textFile("LoremIpsum.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)

print("Total length: %s" % totalLength)

# By default, each transformed RDD may be recomputed each time you run an
# action on it. However, you may also persist an RDD in memory using the
# persist (or cache) method, in which case Spark will keep the elements around
# on the cluster for much faster access the next time you query it.

lineLengths.persist()

sc.stop()
