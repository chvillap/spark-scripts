from pyspark.sql import SparkSession
from pyspark.ml.feature import QuantileDiscretizer

# QuantileDiscretizer takes a column with continuous features and outputs a
# column with binned categorical features. The number of bins is set by the
# numBuckets parameter. It is possible that the number of buckets used will be
# smaller than this value, for example, if there are too few distinct values of
# the input to create enough distinct quantiles.

# NaN values: NaN values will be removed from the column during
# QuantileDiscretizer fitting. This will produce a Bucketizer model for making
# predictions. During the transformation, Bucketizer will raise an error when
# it finds NaN values in the dataset, but the user can also choose to either
# keep or remove NaN values within the dataset by setting handleInvalid. If the
# user chooses to keep NaN values, they will be handled specially and placed
# into their own bucket, for example, if 4 buckets are used, then non-NaN data
# will be put into buckets[0-3], but NaNs will be counted in a special
# bucket[4].

spark = SparkSession.builder.appName("QuantileDiscretizer").getOrCreate()

data = [(0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2)]
df = spark.createDataFrame(data, ["id", "hour"])

discretizer = QuantileDiscretizer(inputCol="hour", outputCol="result",
                                  numBuckets=3)
discretizerModel = discretizer.fit(df)

result = discretizerModel.transform(df)
result.show()

spark.stop()
