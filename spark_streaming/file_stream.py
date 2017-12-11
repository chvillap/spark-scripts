from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# For reading data from files on any file system compatible with the HDFS API
# (that is, HDFS, S3, NFS, etc.), a DStream can be created as:
# streamingContext.textFileStream(dataDirectory)
#
# Spark Streaming will monitor the directory dataDirectory and process any
# files created in that directory (files written in nested directories not
# supported). Note that
#   - The files must have the same data format.
#   - The files must be created in the dataDirectory by atomically moving or
#     renaming them into the data directory.
#   - Once moved, the files must not be changed. So if the files are being
#     continuously appended, the new data will not be read.
#
# File streams do not require running a receiver, hence does not require
# allocating cores.

sc = SparkContext("local[2]", "File Stream")
ssc = StreamingContext(sc, 1)

lines = ssc.textFileStream("fileStreamSource")

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))

counts = pairs.reduceByKey(lambda x, y: x + y)
counts.pprint()

ssc.start()
ssc.awaitTermination()
