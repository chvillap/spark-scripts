from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName('HelloWorld2')
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.collect())

sc.stop()
