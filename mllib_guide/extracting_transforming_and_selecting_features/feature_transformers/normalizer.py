from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import Normalizer

# Normalizer is a Transformer which transforms a dataset of Vector rows,
# normalizing each Vector to have unit norm. It takes parameter p, which
# specifies the p-norm used for normalization (p = 2 by default). This
# normalization can help standardize your input data and improve the behavior
# of learning algorithms.

spark = SparkSession.builder.appName("Normalizer").getOrCreate()

dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.5, -1.0]),),
    (1, Vectors.dense([2.0, 1.0, 1.0]),),
    (2, Vectors.dense([4.0, 10.0, 2.0]),)
], ["id", "features"])

# Normalize each Vector using $L^1$ norm.
normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=1.0)
l1NormData = normalizer.transform(dataFrame)

print("Normalized using L^1 norm")
l1NormData.show()

# Normalize each Vector using L^inf norm.
lInfNormData = normalizer.transform(dataFrame, {normalizer.p: float("inf")})

print("Normalized using L^inf norm")
lInfNormData.show()

spark.stop()
