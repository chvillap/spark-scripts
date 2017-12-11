# Receiving data over the network (like Kafka, Flume, socket, etc.) requires
# the data to be deserialized and stored in Spark. If the data receiving
# becomes a bottleneck in the system, then consider parallelizing the data
# receiving. Note that each input DStream creates a single receiver (running on
# a worker machine) that receives a single stream of data. Receiving multiple
# data streams can therefore be achieved by creating multiple input DStreams
# and configuring them to receive different partitions of the data stream from
# the source(s).

# These multiple DStreams can be unioned together (using
# streamingContex.union()) to create a single DStream. Then the transformations
# that were being applied on a single input DStream can be applied on the
# unified stream.


from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def updateFunc(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


sc = SparkContext("local[4]", "Streaming Multiple Sources")
ssc = StreamingContext(sc, 3)

ssc.checkpoint("checkpoint3")

dstream1 = ssc.socketTextStream("localhost", 9990)
dstream2 = ssc.socketTextStream("localhost", 9991)

dstreamUnion = ssc.union(dstream1, dstream2)

emptyRDD = sc.parallelize([])
wordCounts = dstreamUnion.flatMap(lambda x: x.split(" ")) \
                         .map(lambda x: (x, 1)) \
                         .reduceByKey(lambda x, y: x + y) \
                         .updateStateByKey(updateFunc, initialRDD=emptyRDD)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
