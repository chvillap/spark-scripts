# The spark-submit script in Spark’s bin directory is used to launch
# applications on a cluster. It can use all of Spark’s supported cluster
# managers through a uniform interface so you don’t have to configure your
# application specially for each one.

# If your code depends on other projects, you will need to package them
# alongside your application in order to distribute the code to a Spark
# cluster.

# For Python, you can use the --py-files argument of spark-submit to add .py,
# .zip or .egg files to be distributed with your application. If you depend on
# multiple Python files we recommend packaging them into a .zip or .egg.

# Once a user application is bundled, it can be launched using the
# bin/spark-submit script. This script takes care of setting up the classpath
# with Spark and its dependencies, and can support different cluster managers
# and deploy modes that Spark supports:
#
# ./bin/spark-submit \
#   --class <main-class> \
#   --master <master-url> \
#   --deploy-mode <deploy-mode> \
#   --conf <key>=<value> \
#   ... # (other options)
#   <application-jar> \
#   [application-arguments]
#
# --class:
#   The entry point for your application
#   (e.g. org.apache.spark.examples.SparkPi).
# --master:
#   The master URL for the cluster (e.g. spark://23.195.26.187:7077)
# --deploy-mode:
#   Whether to deploy your driver on the worker nodes (cluster) or locally as
#   an external client (client) (default: client)
# --conf:
#   Arbitrary Spark configuration property in key=value format. For values that
#   contain spaces wrap “key=value” in quotes (as shown).
# application-jar:
#   Path to a bundled jar including your application and all dependencies.
#   The URL must be globally visible inside of your cluster, for instance,
#   an hdfs:// path or a file:// path that is present on all nodes.
# application-arguments:
#   Arguments passed to the main method of your main class, if any.

# The master URL passed to Spark can be in one of the following formats:
# local
#   Run Spark locally with one worker thread (i.e. no parallelism at all).
# local[K]
#   Run Spark locally with K worker threads (ideally, set this to the number
#   of cores on your machine).
# local[*]
#   Run Spark locally with as many worker threads as logical cores on your
#   machine.
# spark://HOST:PORT
#   Connect to the given Spark standalone cluster master. The port must be
#   whichever one your master is configured to use, which is 7077 by default.
# mesos://HOST:PORT
#   Connect to the given Mesos cluster. The port must be whichever one your is
#   configured to use, which is 5050 by default. Or, for a Mesos cluster using
#   ZooKeeper, use mesos://zk://.... To submit with --deploy-mode cluster, the
#   HOST:PORT should be configured to connect to the MesosClusterDispatcher.
# yarn
#   Connect to a YARN cluster in client or cluster mode depending on the value
#   of --deploy-mode. The cluster location will be found based on the
#   HADOOP_CONF_DIR or YARN_CONF_DIR variable.

# Run this example with the command:
# spark-submit --master local[*] MyApp2.py README.md --py-files MyFunctions2.py


def runApp(filename):
    from pyspark import SparkConf
    from pyspark import SparkContext
    from MyFunctions import tokenize
    from MyFunctions import notEmpty
    from MyFunctions import initCount
    from MyFunctions import accumulate
    from MyFunctions import swapPair

    conf = SparkConf().setMaster("local[*]").setAppName("MyApp")
    sc = SparkContext(conf=conf)

    wordCounts = sc.textFile(filename) \
                   .flatMap(tokenize) \
                   .filter(notEmpty) \
                   .map(initCount) \
                   .reduceByKey(accumulate) \
                   .map(swapPair) \
                   .sortByKey(ascending=False) \
                   .map(swapPair)

    wordCounts.cache()
    print(wordCounts.collect())

    sc.stop()


if __name__ == "__main__":
    from sys import argv

    if len(argv) < 2:
        print("Usage: {} filename".format(argv[0]))
    else:
        runApp(argv[1])
