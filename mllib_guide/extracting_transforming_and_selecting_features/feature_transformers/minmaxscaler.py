from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinMaxScaler

# MinMaxScaler transforms a dataset of Vector rows, rescaling each feature to a
# specific range (often [0, 1]). It takes parameters:
#    min: 0.0 by default.
#         Lower bound after transformation, shared by all features.
#    max: 1.0 by default.
#         Upper bound after transformation, shared by all features.

# MinMaxScaler computes summary statistics on a data set and produces a
# MinMaxScalerModel. The model can then transform each feature individually
# such that it is in the given range.

spark = SparkSession.builder.appName("MinMaxScaler").getOrCreate()

dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.1, -1.0]),),
    (1, Vectors.dense([2.0, 1.1, 1.0]),),
    (2, Vectors.dense([3.0, 10.1, 3.0]),)
], ["id", "features"])

scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

# Compute summary statistics and generate MinMaxScalerModel.
scalerModel = scaler.fit(dataFrame)

# Rescale each feature to range [min, max].
scaledData = scalerModel.transform(dataFrame)

print("Features scaled to range: [%f, %f]" %
      (scaler.getMin(), scaler.getMax()))

scaledData.select("features", "scaledFeatures").show()

spark.stop()
