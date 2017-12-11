from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.streaming import StreamingContext

# You can easily use DataFrames and SQL operations on streaming data. You have
# to create a SparkSession using the SparkContext that the StreamingContext is
# using. Furthermore this has to done such that it can be restarted on driver
# failures. This is done by creating a lazily instantiated singleton instance
# of SparkSession.


# Lazily instantiated global instance of SparkSession.
def getSparkSessionInstance(sparkConf):
    if "sparkSessionSingletonInstance" not in globals():
        globals()["sparkSessionSingletonInstance"] = \
            SparkSession.builder.config(conf=sparkConf).getOrCreate()

    return globals()["sparkSessionSingletonInstance"]


# DataFrame operations inside your streaming program.
def process(time, rdd):
    print("========= %s =========" % str(time))

    # We can't create DataFrame from an empty RDD.
    if rdd.isEmpty():
        return

    # Get the singleton instance of SparkSession.
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Convert RDD[String] to RDD[Row] to DataFrame.
    rowRDD = rdd.map(lambda word: Row(word=word))
    wordsDF = spark.createDataFrame(rowRDD)

    # Create temporary view using the DataFrame.
    wordsDF.createOrReplaceTempView("words")

    # Do word count on table using SQL and print it.
    wordCountsDF = spark.sql("""
        SELECT word, COUNT(*) as total
        FROM words
        GROUP BY word""")

    wordCountsDF.show()


sc = SparkContext("local[2]", "DataFrame and SQL Operations")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))

words.foreachRDD(process)

ssc.start()
ssc.awaitTermination()

# You can also run SQL queries on tables defined on streaming data from a
# different thread (that is, asynchronous to the running StreamingContext).
# Just make sure that you set the StreamingContext to remember a sufficient
# amount of streaming data such that the query can run. Otherwise the
# StreamingContext, which is unaware of the any asynchronous SQL queries,
# will delete off old streaming data before the query can complete. For
# example, if you want to query the last batch, but your query can take 5
# minutes to run, then call streamingContext.remember(Minutes(5)).
