from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

sc = SparkContext("local", "Parquet Schema Merging")
ss = SparkSession.builder.appName("Parquet Schema Merging").getOrCreate()

# Table partitioning is a common optimization approach used in systems like
# Hive. In a partitioned table, data are usually stored in different
# directories, with partitioning column values encoded in the path of each
# partition directory. The Parquet data source is now able to discover and
# infer partitioning information automatically.

# Like ProtocolBuffer, Avro, and Thrift, Parquet also supports schema
# evolution. Users can start with a simple schema, and gradually add more
# columns to the schema as needed. In this way, users may end up with multiple
# Parquet files with different but mutually compatible schemas. The Parquet
# data source is now able to automatically detect this case and merge schemas
# of all these files.

# Since schema merging is a relatively expensive operation, and is not a
# necessity in most cases, we turned it off by default starting from 1.5.0.
# You may enable it by:
#   1. Setting data source option mergeSchema to true when reading Parquet
#      files (as shown in the examples below), OR
#   2. Setting the global SQL option spark.sql.parquet.mergeSchema to true.

# Create a simple DataFrame, stored into a partition directory.
squaresDF = ss.createDataFrame(sc.parallelize(range(1, 6))
                                 .map(lambda i: Row(single=i, double=i**2)))
squaresDF.write.parquet("data/test_table/key=1")

# Create a new DataFrame in a new partition directory, adding a new column
# and dropping an existing column.
cubesDF = ss.createDataFrame(sc.parallelize(range(6, 11))
                               .map(lambda i: Row(single=i, triple=i**3)))
cubesDF.write.parquet("data/test_table/key=2")

# Read the partitioned table.
mergedDF = ss.read.option("mergeSchema", "true").parquet("data/test_table")
mergedDF.printSchema()
mergedDF.show()

ss.stop()
sc.stop()

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
#  |-- double: long (nullable = true)
#  |-- single: long (nullable = true)
#  |-- triple: long (nullable = true)
#  |-- key: integer (nullable = true)
