from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and batch interval
# of 1 second.
batch_interval = 1
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, batch_interval)

# Create a DStream that will connect to hostname:port, like localhost:9999.
hostname = "localhost"
port = 9999
lines = ssc.socketTextStream(hostname, port)
# Each record in this DStream is a line of text.

# Split each line into words.
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch.
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the
# console.
wordCounts.pprint()

# Start the computation.
ssc.start()

# Wait for the computation to terminate.
ssc.awaitTermination()
