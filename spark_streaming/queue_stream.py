from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import os

# For testing a Spark Streaming application with test data, one can also create
# a DStream based on a queue of RDDs, using:
# streamingContext.queueStream(queueOfRDDs)
# Each RDD pushed into the queue will be treated as a batch of data in the
# DStream, and processed like a stream.

sc = SparkContext("local[2]", "Queue Stream")
ssc = StreamingContext(sc, 1)

rddQueue = []
for i in range(1, 6):
    rdd = sc.textFile(os.path.join("queueStreamSource", "%02d.txt" % i))
    rddQueue.append(rdd)

lines = ssc.queueStream(rddQueue, oneAtATime=True)  # one file at a time
# lines = ssc.queueStream(rddQueue, oneAtATime=False)  # all files at once

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
