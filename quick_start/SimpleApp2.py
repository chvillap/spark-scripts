"""SimpleApp2.py
"""

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster('local[2]').setAppName('SimpleApp')
sc = SparkContext(conf=conf)

filename = 'README.md'
textRdd = sc.textFile(filename).cache()

wordsRdd = textRdd.flatMap(lambda x: x.split(' '))
pairsRdd = wordsRdd.map(lambda x: (x, 1))
countsRdd = pairsRdd.reduceByKey(lambda x, y: x + y)

count = countsRdd.count()
print(countsRdd.takeOrdered(count, key=lambda x: -x[1]))

# sortedCountsRdd = countsRdd.map(lambda x: (x[1], x[0])) \
#                            .sortByKey(ascending=False) \
#                            .map(lambda x: (x[1], x[0]))
# print(sortedCountsRdd.collect())

sc.stop()
