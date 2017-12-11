from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "JSON 1")
ss = SparkSession(sc)

# Spark SQL can automatically infer the schema of a JSON dataset and load it as
# a DataFrame. This conversion can be done using SparkSession.read.json on a
# JSON file.
# Note that the file that is offered as a json file is not a typical JSON file.
# Each line must contain a separate, self-contained valid JSON object. As a
# consequence, a regular multi-line JSON file will most often fail.

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
peopleDF = ss.read.json("people.json")

# The inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()

# Creates a temporary view using the DataFrame.
peopleDF.createOrReplaceTempView("people")

# SQL statements can be run by using the sql methods provided by ss.
teenNamesDF = ss.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
teenNamesDF.show()

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
jsonStrings = [
    '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}',
    '{"name":"Carlos","address":{"city":"Araraquara","state":"São Paulo"}}',
]
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = ss.read.json(otherPeopleRDD)
otherPeople.show()

ss.stop()
sc.stop()
