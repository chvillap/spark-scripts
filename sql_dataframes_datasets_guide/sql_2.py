from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("SQL 2").setMaster("local")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName("SQL 2").getOrCreate()

# Instead of using read API to load a file into DataFrame and query it,
# you can also query that file directly with SQL.

# df1 = ss.sql("SELECT * FROM parquet.`path/to/file/users.parquet`")
df1 = ss.sql("SELECT * FROM parquet.`users.parquet`")
df1.show()

# df2 = ss.sql("SELECT * FROM json.`path/to/file/people.json`")
df2 = ss.sql("SELECT * FROM json.`people.json`")
df2.show()

ss.stop()
sc.stop()
