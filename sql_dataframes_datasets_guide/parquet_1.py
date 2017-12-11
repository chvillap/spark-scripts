from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Parquet Files 1")
ss = SparkSession.builder.appName("Parquet Files 1").getOrCreate()

peopleDF = ss.read.json("people.json")

# Parquet is a columnar format that is supported by many other data processing
# systems. Spark SQL provides support for both reading and writing Parquet
# files that automatically preserves the schema of the original data. When
# writing Parquet files, all columns are automatically converted to be nullable
# for compatibility reasons.

# DataFrames can be saved as Parquet files, maintaining the schema information.
peopleDF.write.parquet("people.parquet")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = ss.read.parquet("people.parquet")

# Parquet files can also be used to create a temporary view and then used in
# SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
teens = ss.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
teens.show()

ss.stop()
sc.stop()
