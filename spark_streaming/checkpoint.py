from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# A streaming application must operate 24/7 and hence must be resilient to
# failures unrelated to the application logic (e.g., system failures, JVM
# crashes, etc.). For this to be possible, Spark Streaming needs to checkpoint
# enough information to a fault- tolerant storage system such that it can
# recover from failures.

# Metadata checkpointing is primarily needed for recovery from driver failures,
# whereas data or RDD checkpointing is necessary even for basic functioning if
# stateful transformations (updateStateByKey, reduceByKeyAndWindow) are used.

# Checkpointing can be enabled by setting a directory in a fault-tolerant,
# reliable file system (e.g., HDFS, S3, etc.) to which the checkpoint
# information will be saved. This is done by using
# streamingContext.checkpoint(checkpointDirectory). This will allow you to
# use the aforementioned stateful transformations. Additionally, if you want
# to make the application recover from driver failures, you should rewrite
# your streaming application to have the following behavior.
#   1. When the program is being started for the first time, it will create a
#      new StreamingContext, set up all the streams and then call start().
#   2. When the program is being restarted after failure, it will re-create a
#      StreamingContext from the checkpoint data in the checkpoint directory.

batch_interval = 1
hostname, port = "localhost", 9999
checkpointDirectory = "checkpoint"


# Function to create and setup a new StreamingContext.
def createContext():
    # If you do not see this printed, that means the StreamingContext has been
    # loaded from the new checkpoint.
    print("===== Creating new context ======")

    sc = SparkContext("local[2]", "Checkpoint")
    ssc = StreamingContext(sc, batch_interval)

    lines = ssc.socketTextStream(hostname, port)

    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))

    emptyRDD = sc.parallelize([])
    wordCounts = pairs.updateStateByKey(
        lambda newValues, runningCount: sum(newValues, runningCount or 0),
        initialRDD=emptyRDD)

    wordCounts.pprint()

    ssc.checkpoint(checkpointDirectory)  # set checkpoint directory.
    return ssc


# Get StreamingContext from checkpoint data or create a new one.
context = StreamingContext.getOrCreate(checkpointDirectory, createContext)

# Do additional setup on context that needs to be done,
# irrespective of whether it is being started or restarted.
# ...

# Start the context.
context.start()
context.awaitTermination()

# You can also explicitly create a StreamingContext from the checkpoint data
# and start the computation by using
# StreamingContext.getOrCreate(checkpointDirectory, None).

# Note that checkpointing of RDDs incurs the cost of saving to reliable
# storage. This may cause an increase in the processing time of those batches
# where RDDs get checkpointed. Hence, the interval of checkpointing needs to be
# set carefully. At small batch sizes (say 1 second), checkpointing every batch
# may significantly reduce operation throughput. Conversely, checkpointing too
# infrequently causes the lineage and task sizes to grow, which may have
# detrimental effects. For stateful transformations that require RDD
# checkpointing, the default interval is a multiple of the batch interval that
# is at least 10 seconds. It can be set by using
# dstream.checkpoint(checkpointInterval). Typically, a checkpoint interval of
# 5-10 sliding intervals of a DStream is a good setting to try.
