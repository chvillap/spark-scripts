"""SimpleApp.py
"""

from pyspark import SparkContext

sc = SparkContext('local[2]', 'Simple App')

logFile = 'README.md'
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda line: 'a' in line).count()
numBs = logData.filter(lambda line: 'b' in line).count()

print('Lines with a: %i' % numAs)
print('Lines with b: %i' % numBs)

sc.stop()
