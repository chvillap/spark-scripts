# Covariance is a measure of how two variables change with respect to each
# other. A positive number would mean that there is a tendency that as one
# variable increases, the other increases as well. A negative number would
# mean that as one variable increases, the other variable has a tendency to
# decrease. The sample covariance of two columns of a DataFrame can be
# calculated as follows:

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import rand

spark = SparkSession \
    .builder \
    .appName("Summary and descriptive statistics") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

df = sqlContext \
    .range(0, 10) \
    .withColumn("rand1", rand(seed=10)) \
    .withColumn("rand2", rand(seed=27))

# The covariance of the two randomly generated columns is close to zero,
# while the covariance of the id column with itself is very high.
print(df.stat.cov("rand1", "rand2"))
print(df.stat.cov("id", "id"))

# The covariance value of 9.17 might be hard to interpret. Correlation
# is a normalized measure of covariance that is easier to understand, as
# it provides quantitative measurements of the statistical dependence
# between two random variables.
print(df.stat.corr("rand1", "rand2"))
print(df.stat.corr("id", "id"))

spark.stop()
