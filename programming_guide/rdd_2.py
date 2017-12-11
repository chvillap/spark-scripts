from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("RDDs 2").setMaster("local")
sc = SparkContext(conf=conf)

# Text file RDDs can be created using SparkContext's textFile() method.
# This method takes an URI for the file (either a local path on the machine,
# or a hdfs://, s3n://, etc URI) and reads it as a collection of lines.

distFile = sc.textFile("ex1data1.txt")

print(distFile.take(5))

# If using a path on the local filesystem, the file must also be accessible at
# the same path on worker nodes. Either copy the file to all workers or use a
# network-mounted shared file system.
# All of Spark’s file-based input methods, including textFile, support running
# on directories, compressed files, and wildcards as well.

# SparkContext.wholeTextFiles lets you read a directory containing multiple
# small text files, and returns each of them as (filename, content) pairs.

distFiles = sc.wholeTextFiles("files")

print(distFiles.collect())
