from pyspark import SparkConf
from pyspark import SparkContext

# A name for your application to show on the cluster UI.
APP_NAME = "HelloWorld"

# A Spark, Mesos or YARN cluster URL.
# Or a special "local" string to run in local mode.
MASTER = "local"

# The first thing a Spark program must do is to create a SparkContext object,
# which tells Spark how to access a cluster.
# To create a SparkContext you first need to build a SparkConf object that
# contains information about your application.
conf = SparkConf().setAppName(APP_NAME).setMaster(MASTER)
sc = SparkContext(conf=conf)

print("Hello, world!")

sc.stop()

# In practice, when running on a cluster, you will not want to hardcode master
# in the program, but rather launch the application with spark-submit and
# receive it there.
