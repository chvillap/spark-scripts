from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorSlicer

# VectorSlicer is a transformer that takes a feature vector and outputs a new
# feature vector with a sub-array of the original features. It is useful for
# extracting features from a vector column.

# VectorSlicer accepts a vector column with specified indices, then outputs a
# new vector column whose values are selected via those indices. There are two
# types of indices,
#   - Integer indices that represent the indices into the vector, setIndices().
#   - String indices that represent the names of features into the vector,
#     setNames(). This requires the vector column to have an AttributeGroup
#     since the implementation matches on the name field of an Attribute.

# The output vector will order features with the selected indices first (in
# the order given), followed by the selected names (in the order given).

spark = SparkSession.builder.appName("VectorSlicer").getOrCreate()

df = spark.createDataFrame([
    Row(userFeatures=Vectors.sparse(3, {0: -2.0, 1: 2.3})),
    Row(userFeatures=Vectors.dense([-2.0, 2.3, 0.0]))])

slicer = VectorSlicer(inputCol="userFeatures", outputCol="features",
                      indices=[1])

output = slicer.transform(df)
output.select("userFeatures", "features").show()

spark.stop()
