from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "JDBC 1")
ss = SparkSession.builder.appName("JDBC 1").getOrCreate()

# Spark SQL also includes a data source that can read data from other databases
# (i.e. not only Hive) using JDBC.
# To get started you will need to include the JDBC driver for you particular
# database on the spark classpath.
#
# ./bin/spark-submit --driver-class-path [driver-jar] --jars [driver-jar]

# Note: JDBC loading and saving can be achieved via either the load/save or
# jdbc methods.

# # Loading data from a JDBC source (1).
# df1 = ss.read \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql:dbserver") \
#         .option("dbtable", "schema.tablename") \
#         .option("user", "username") \
#         .option("password", "password") \
#         .load()

# # Loading data from a JDBC source (2).
# df2 = ss.read \
#         .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#               properties={"user": "username", "password": "password"})

# Loading data from a JDBC source (1).
df1 = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:my_database.db") \
        .option("dbtable", "cars") \
        .load()
df1.printSchema()

# Loading data from a JDBC source (2).
df2 = ss.read.jdbc("jdbc:sqlite:my_database.db", "cars")
df2.printSchema()

# Some SQL queries.
df1.createOrReplaceTempView("cars")
ss.sql("SELECT * FROM cars WHERE class = 'vgood'").show()
ss.sql("SELECT safety, class FROM cars WHERE doors = '5more'").show()

# Some DataFrame operations.
df2.filter(df2["class"] == "vgood").show()
df2.filter(df2["doors"] == "5more").select(["safety", "class"]).show()

ss.stop()
sc.stop()
