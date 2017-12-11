from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("Accumulator 3").setMaster("local")
sc = SparkContext(conf=conf)

accum = sc.accumulator(0)


# Accumulators do not change the lazy evaluation model of Spark. If they are
# being updated within an operation on an RDD, their value is only updated once
# that RDD is computed as part of an action. Consequently, accumulator updates
# are not guaranteed to be executed when made within a lazy transformation
# like map().

def g(x):
    accum.add(x)
    return x ** 2


rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = rdd1.map(g)

# Here, accum is still 0 because no actions have caused map to be computed.
print(accum)

rdd2.take(1)  # accum.value == 1
rdd2.take(2)  # accum.value == 3
rdd2.take(3)  # accum.value == 6
rdd2.take(4)  # accum.value == 10
# rdd2.collect()  # accum.value == 10

# Here, accum is above 0 because the take/collect action has caused map to be
# computed. Notice that we can get only a partial accumulated result if the
# action requires only a portion of the data to be computed.
print(accum.value)

sc.stop()
