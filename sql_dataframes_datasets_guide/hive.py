from pyspark.sql import SparkSession
from pyspark.sql import Row

# Spark SQL also supports reading and writing data stored in Apache Hive.
# However, since Hive has a large number of dependencies, these dependencies
# are not included in the default Spark distribution. If Hive dependencies can
# be found on the classpath, Spark will load them automatically. Note that
# these Hive dependencies must also be present on all of the worker nodes, as
# they will need access to the Hive serialization and deserialization libraries
# (SerDes) in order to access data stored in Hive.

# When working with Hive, one must instantiate SparkSession with Hive support,
# including connectivity to a persistent Hive metastore, support for Hive
# serdes, and Hive user-defined functions.

# warehouse_location points to the default location for managed databases and
# tables.
warehouse_location = "spark-warehouse"

ss = SparkSession.builder                                               \
                 .appName("Hive")                                       \
                 .config("spark.sql.warehouse.dir", warehouse_location) \
                 .enableHiveSupport()                                   \
                 .getOrCreate()

ss.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
ss.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE src")

# Queries are expressed in HiveQL.
ss.sql("SELECT * FROM src").show()

# Aggregation queries are also supported.
ss.sql("SELECT COUNT(*) FROM src").show()

# The results of SQL queries are themselves DataFrames and support all normal
# functions.
sqlDF = ss.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

# The items in DaraFrames are of type Row, which allows you to access each
# column by ordinal.
strDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))
for record in strDS.collect():
    print(record)

# You can also use DataFrames to create temporary views within a SparkSession.
Record = Row("key", "value")
recordsDF = ss.createDataFrame(
    [Record(i, "val_" + str(i)) for i in range(1, 101)])
recordsDF.createOrReplaceTempView("records")

# Queries can then join DataFrame data with data stored in Hive.
ss.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

ss.stop()
