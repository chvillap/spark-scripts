from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("RDDs 3").setMaster("local")
sc = SparkContext(conf=conf)

OUTPUT_PATH = "output"

# Similarly to text files, SequenceFiles can be saved and loaded by specifying
# the path. The key and value classes can be specified, but for standard
# Writables this is not required.

rdd1 = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd1.saveAsSequenceFile(OUTPUT_PATH)

rdd2 = sorted(sc.sequenceFile(OUTPUT_PATH).collect())
print(rdd2)
