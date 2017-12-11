from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Global TempView").setMaster("local")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName("Global TempView").getOrCreate()

df = ss.read.json("people.json")

# Temporary views in Spark SQL are session-scoped and will disappear if the
# session that creates it terminates. If you want to have a temporary view
# that is shared among all sessions and keep alive until the Spark application
# terminates, you can create a global temporary view. Global temporary view is
# tied to a system preserved database global_temp, and we must use the
# qualified name to refer it, e.g. SELECT * FROM global_temp.view1.

# Register the DataFrame as a global temporary view.
df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database 'global_temp'.
ss.sql("SELECT * FROM global_temp.people").show()

# Global temporary view is cross-session.
ss.newSession().sql("SELECT * FROM global_temp.people").show()

ss.stop()
sc.stop()
