from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer

# Bucketizer transforms a column of continuous features to a column of feature
# buckets, where the buckets are specified by users. It takes a parameter:
#    splits: Parameter for mapping continuous features into buckets. With n+1
#            splits, there are n buckets. A bucket defined by splits x,y holds
#            values in the range [x,y) except the last bucket, which also
#            includes y. Splits should be strictly increasing. Values at -inf,
#            inf must be explicitly provided to cover all Double values;
#            Otherwise, values outside the splits specified will be treated as
#            errors.

# Note that if you have no idea of the upper and lower bounds of the targeted
# column, you should add Double.NegativeInfinity and Double.PositiveInfinity as
# the bounds of your splits to prevent a potential out of Bucketizer bounds
# exception.

# Note also that the splits that you provided have to be in strictly increasing
# order, i.e. s0 < s1 < s2 < ... < sn.

spark = SparkSession.builder.appName("Bucketizer").getOrCreate()

splits = [-float("inf"), -0.5, 0.0, 0.5, float("inf")]

data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
dataFrame = spark.createDataFrame(data, ["features"])

bucketizer = Bucketizer(inputCol="features", outputCol="bucketedFeatures",
                        splits=splits)

# Transform original data into its bucket index.
bucketedData = bucketizer.transform(dataFrame)

print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits()) - 1))
bucketedData.show()

spark.stop()
