from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import AccumulatorParam
from pyspark.mllib.linalg import Vectors

conf = SparkConf().setAppName("Accumulator 1").setMaster("local")
sc = SparkContext(conf=conf)

# Spark natively supports accumulators of numeric types, and programmers can
# add support for new types.

# Programmers can create their own types by subclassing AccumulatorParam. The
# AccumulatorParam interface has two methods: zero for providing a "zero value"
# for your data type, and addInPlace for adding two values together.


class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return Vectors.dense([0] * initialValue.size)

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1


accum = sc.accumulator(Vectors.dense([10, 20, 30]), VectorAccumulatorParam())

# For accumulator updates performed inside actions only, Spark guarantees that
# each task's update to the accumulator will only be applied once, i.e.
# restarted tasks will not update the value.

rdd = sc.parallelize([1, 2, 3, 4])
rdd.foreach(lambda x: accum.add(Vectors.dense([x, x, x])))

print(accum.value)

sc.stop()
