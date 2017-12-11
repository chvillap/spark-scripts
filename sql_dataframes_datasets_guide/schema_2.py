from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

conf = SparkConf().setAppName("Schema Inference").setMaster("local")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName("Schema Inference").getOrCreate()

# When a dictionary of kwargs cannot be defined ahead of time (for example,
# the structure of records is encoded in a string, or a text dataset will be
# parsed and fields will be projected differently for different users), a
# DataFrame can be created programmatically with three steps.
#   1. Create an RDD of tuples or lists from the original RDD;
#   2. Create the schema represented by a StructType matching the structure
#      of tuples or lists in the RDD created in the step 1.
#   3. Apply the schema to the RDD via createDataFrame method provided by
#      SparkSession.

lines = sc.textFile("people.txt")
parts = lines.map(lambda s: s.split(","))
# Each line is converted to a tuple.
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True)
          for field_name in schemaString.split()]
schema = StructType(fields)

# Apply schema to the RDD.
schemaPeople = ss.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame.
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = ss.sql("SELECT name FROM people")
results.show()

ss.stop()
sc.stop()
