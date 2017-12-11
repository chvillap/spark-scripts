from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

conf = SparkConf().setAppName("Schema Inference").setMaster("local")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName("Schema Inference").getOrCreate()

# Spark SQL can convert an RDD of Row objects to a DataFrame, inferring the
# datatypes. Rows are constructed by passing a list of key/value pairs as
# kwargs to the Row class. The keys of this list define the column names of
# the table, and the types are inferred by sampling the whole dataset, similar
# to the inference that is performed on JSON files.
# This reflection based approach leads to more concise code and works well when
# you already know the schema while writing your Spark application.

# Load a text file and convert each line to a Row.
lines = sc.textFile("people.txt")
parts = lines.map(lambda s: s.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = ss.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = ss.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as a pyspark.RDD of Row.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()

for name in teenNames:
    print(name)

ss.stop()
sc.stop()
