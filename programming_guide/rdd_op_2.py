from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("RDD Operations 2").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.textFile("LoremIpsum.txt")

# Note that while it is also possible to pass a reference to a method in a
# class instance (as opposed to a singleton object), this requires sending
# the object that contains that class along with the method.


class MyClass(object):
    def __init__(self):
        self.field = "[Start] "

    def func(self, s):
        return self.field + s

    def doStuff(self, rdd):
        # Here, if we create a new MyClass and call doStuff on it, the
        # map inside there references the func method of that MyClass
        # instance, so the whole object needs to be sent to the cluster.
        # In a similar way, accessing fields of the outer object would
        # reference the whole object.
        # return rdd.map(self.func)

        # To avoid this issue, the simplest way is to copy field into a
        # local variable instead of accessing it externally.
        field = self.field
        return rdd.map(lambda s: field + s)


result = MyClass().doStuff(rdd)
print(result.collect())


sc.stop()
