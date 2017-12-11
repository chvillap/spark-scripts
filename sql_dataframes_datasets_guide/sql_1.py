from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("SQL 1").setMaster("local")
sc = SparkContext(conf=conf)

ss = SparkSession.builder.appName("SQL 1").getOrCreate()

# The sql function on a SparkSession enables applications to run SQL queries
# programmatically and returns the result as a DataFrame.

df = ss.read.json("people.json")

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

sqlDF = ss.sql("SELECT * FROM people")
sqlDF.show()

ss.stop()
sc.stop()
