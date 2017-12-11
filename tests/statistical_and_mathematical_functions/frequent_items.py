# Figuring out which items are frequent in each column can be very useful to
# understand a dataset. We have implemented an one-pass algorithm proposed by
# Karp et al. This is a fast, approximate algorithm that always return all
# the frequent items that appear in a user-specified minimum proportion of
# rows. Note that the result might contain false positives, i.e. items that
# are not frequent.

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import struct

spark = SparkSession \
    .builder \
    .appName("Summary and descriptive statistics") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

df = sqlContext.createDataFrame(
    [(1, 2, 3) if i % 2 == 0 else (i, 2 * i, i % 4) for i in range(100)],
    ["a", "b", "c"])
df.show(10)

# Given the above DataFrame, the following code finds the frequent items
# that show up 40% of the time for each column:
freq = df.stat.freqItems(["a", "b", "c"], 0.4)
freq.collect()[0]

# You can also find frequent items for column combinations, by creating
# a composite column using the struct function:
freq = df.withColumn('ab', struct('a', 'b')).stat.freqItems(['ab'], 0.4)
freq.collect()[0]

# From the above example, the combination of "a=11 and b=22", and
# "a=1 and b=2" appear frequently in this dataset. Note that "a=11 and b=22"
# is a false positive.

spark.stop()
