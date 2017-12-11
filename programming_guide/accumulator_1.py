from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("Accumulator 1").setMaster("local")
sc = SparkContext(conf=conf)

# Accumulators are variables that are only "added" to through an associative
# and commutative operation and can therefore be efficiently supported in
# parallel. They can be used to implement counters (as in MapReduce) or sums.

accum = sc.accumulator(0)
print(accum)

# Tasks running on a cluster can add to an accumulator using the add method or
# the += operator. However, they cannot read its value. Only the driver program
# can read the accumulator’s value, using its value method.

rdd = sc.parallelize([1, 2, 3, 4])
rdd.foreach(lambda x: accum.add(x))

print(accum.value)

sc.stop()
