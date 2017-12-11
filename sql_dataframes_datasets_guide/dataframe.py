from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Dataframes").setMaster("local")
sc = SparkContext(conf=conf)

ss = SparkSession.builder.appName("Dataframes").getOrCreate()

# With a SparkSession, applications can create DataFrames from an existing RDD,
# from a Hive table, or from Spark data sources.
df = ss.read.json("people.json")

# Displays the content of the DataFrame to stdout.
df.show()

ss.stop()
sc.stop()
