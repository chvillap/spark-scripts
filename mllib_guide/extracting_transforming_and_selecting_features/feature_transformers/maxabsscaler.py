from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MaxAbsScaler

# MaxAbsScaler transforms a dataset of Vector rows, rescaling each feature to
# range [-1, 1] by dividing through the maximum absolute value in each feature.
# It does not shift/center the data, and thus does not destroy any sparsity.

# MaxAbsScaler computes summary statistics on a data set and produces a
# MaxAbsScalerModel. The model can then transform each feature individually to
# range [-1, 1].

spark = SparkSession.builder.appName("MaxAbsScaler").getOrCreate()

dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.1, -8.0]),),
    (1, Vectors.dense([2.0, 1.0, -4.0]),),
    (2, Vectors.dense([4.0, 10.0, 8.0]),)
], ["id", "features"])

scaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")

# Compute summary statistics and generate MaxAbsScalerModel.
scalerModel = scaler.fit(dataFrame)

# rescale each feature to range [-1, 1].
scaledData = scalerModel.transform(dataFrame)
scaledData.select("features", "scaledFeatures").show()
