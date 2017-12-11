from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("Broadcast").setMaster("local")
sc = SparkContext(conf=conf)

# Broadcast variables allow the programmer to keep a read-only variable cached
# on each machine rather than shipping a copy of it with tasks.

# Explicitly creating broadcast variables is only useful when tasks across
# multiple stages need the same data or when caching the data in deserialized
# form is important.

bcast = sc.broadcast([1, 2, 3])
print(bcast)

print(bcast.value)

# After the broadcast variable is created, it should be used instead of the
# value v in any functions run on the cluster so that v is not shipped to the
# nodes more than once. In addition, the object v should not be modified after
# it is broadcast in order to ensure that all nodes get the same value of the
# broadcast variable (e.g. if the variable is shipped to a new node later).

sc.stop()
