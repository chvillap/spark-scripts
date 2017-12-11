from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# The updateStateByKey operation allows you to maintain arbitrary state while
# continuously updating it with new information. To use this, you will have to
# do two steps.
#   1. Define the state - The state can be an arbitrary data type.
#   2. Define the state update function - Specify with a function how to update
#      the state using the previous state and the new values from an input
#      stream.

# In every batch, Spark will apply the state update function for all existing
# keys, regardless of whether they have new data in a batch or not. If the
# update function returns None then the key-value pair will be eliminated.

sc = SparkContext("local[2]", "UpdateStateByKey Example")
ssc = StreamingContext(sc, 1)

# Using updateStateByKey requires the checkpoint directory to be configured.
ssc.checkpoint("checkpoint")

initialStateRDD = sc.parallelize([("hello", 1), ("world", 1)])


def updateFunc(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


lines = ssc.socketTextStream("localhost", 9999)
counts = lines.flatMap(lambda line: line.split(" ")) \
              .map(lambda word: (word, 1)) \
              .updateStateByKey(updateFunc, initialRDD=initialStateRDD)

counts.pprint()

ssc.start()
ssc.awaitTermination()
