from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "JDBC 2")
ss = SparkSession.builder.appName("JDBC 2").getOrCreate()

# Spark SQL also includes a data source that can read data from other databases
# (i.e. not only Hive) using JDBC.
# To get started you will need to include the JDBC driver for you particular
# database on the spark classpath.
#
# ./bin/spark-submit --driver-class-path [driver-jar] --jars [driver-jar]

# Note: JDBC loading and saving can be achieved via either the load/save or
# jdbc methods.

# # Loading data from a JDBC source (1).
# df = ss.read.format("jdbc") \
#             .option("url", "jdbc:sqlite:my_database.db") \
#             .option("dbtable", "cars") \
#             .load()

# Loading data from a JDBC source (2).
df = ss.read.jdbc("jdbc:sqlite:my_database.db", "cars")

df.printSchema()

# # Saving data to a JDBC source (1).
# jdbcDF1 = ss.write \
#             .format("jdbc") \
#             .option("url", "jdbc:postgresql:dbserver") \
#             .option("dbtable", "schema.tablename") \
#             .option("user", "username") \
#             .option("password", "password") \
#             .save()

# # Saving data to a JDBC source (2).
# jdbcDF2 = ss.write \
#             .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#                   properties={"user": "username", "password": "password"})

# # Saving data to a JDBC source (1).
# df.write.format("jdbc") \
#         .option("url", "jdbc:sqlite:my_database.db") \
#         .option("dbtable", "cars") \
#         .save()

# Saving data to a JDBC source (2).
df.write.mode("overwrite").jdbc("jdbc:sqlite:my_database", "cars")

ss.stop()
sc.stop()
