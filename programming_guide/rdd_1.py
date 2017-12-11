from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("RDDs 1").setMaster("local")
sc = SparkContext(conf=conf)

# There are two ways to create RDDs: parallelizing an existing collection in
# your driver program, or referencing a dataset in an external storage system,
# such as a shared filesystem, HDFS, HBase, or any data source offering a
# HadoopInputFormat.

# Parallelized collections are created by calling SparkContext's parallelize()
# method on an existing iterable or collection in your driver program.

data = [1, 2, 3, 4, 5]
distData1 = sc.parallelize(data)

print(distData1.collect())

# Spark will run one task for each partition of the cluster. Typically you want
# 2-4 partitions for each CPU in your cluster. Normally, Spark tries to set the
# number of partitions automatically based on your cluster. However, you can
# also set it manually by passing it as a second parameter to parallelize().

distData2 = sc.parallelize(data, 10)

print(distData1.collect())

sc.stop()
