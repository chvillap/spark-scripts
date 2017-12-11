from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# To initialize a Spark Streaming program, a StreamingContext object has to be
# created which is the main entry point of all Spark Streaming functionality.

master = "local[2]"
appName = "Initializing StreamingContext"
sc = SparkContext(master, appName)

batch_interval = 1
ssc = StreamingContext(sc, batch_interval)

# After a context is defined, you have to do the following.
#   1. Define the input sources by creating input DStreams.
#   2. Define the streaming computations by applying transformation and output
#      operations to DStreams.
#   3. Start receiving data and processing it using streamingContext.start().
#   4. Wait for the processing to be stopped (manually or due to any error)
#      using streamingContext.awaitTermination().
#   5. The processing can be manually stopped using streamingContext.stop().

# Points to remember:
#   - Once a context has been started, no new streaming computations can be set
#     up or added to it.
#   - Once a context has been stopped, it cannot be restarted.
#   - Only one StreamingContext can be active in a JVM at the same time.
#   - stop() on StreamingContext also stops the SparkContext. To stop only the
#     StreamingContext, set the optional parameter of stop() called
#     stopSparkContext to false.
#   - A SparkContext can be re-used to create multiple StreamingContexts, as
#     long as the previous StreamingContext is stopped (without stopping the
#     SparkContext) before the next StreamingContext is created.
#   - When running a Spark Streaming program locally, do not use "local" or
#     "local[1]" as the master URL. Either of these means that only one thread
#     will be used for running tasks locally. If you are using an input DStream
#     based on a receiver (e.g. sockets, Kafka, Flume, etc.), then the single
#     thread will be used to run the receiver, leaving no thread for processing
#     the received data. Hence, when running locally, always use "local[n]" as
#     the master URL, where n > number of receivers to run.
