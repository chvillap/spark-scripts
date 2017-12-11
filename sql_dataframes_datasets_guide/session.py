from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Spark Session").setMaster("local")
sc = SparkContext(conf=conf)

# The entry point into all functionality in Spark is the SparkSession class.
# To create a basic SparkSession, just use SparkSession.builder.

ss = SparkSession.builder                                          \
                 .appName("Spark Session")                         \
                 .config("spark.some.config.option", "some-value") \
                 .getOrCreate()

print("SparkSession ready")

ss.stop()
sc.stop()
