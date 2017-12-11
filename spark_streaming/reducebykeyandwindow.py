from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "Window Operations 1")
ssc = StreamingContext(sc, 1)

ssc.checkpoint("checkpoint")

lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))

# Reduce last 30 seconds of data, every 10 seconds.
windowLength = 10
slideInterval = 5
windowedCounts = pairs.reduceByKeyAndWindow(lambda a, b: a + b,  # func
                                            lambda a, b: a - b,  # invFunc
                                            windowLength,
                                            slideInterval)

windowedCounts.pprint()

ssc.start()
ssc.awaitTermination()
