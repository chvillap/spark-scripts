from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import ElementwiseProduct

# ElementwiseProduct multiplies each input vector by a provided "weight"
# vector, using element-wise multiplication. In other words, it scales each
# column of the dataset by a scalar multiplier. This represents the Hadamard
# product between the input vector, v and transforming vector, w, to yield a
# result vector.

spark = SparkSession.builder.appName("ElementwiseProduct").getOrCreate()

# Create some vector data; also works for sparse vectors.
data = [(Vectors.dense([1.0, 2.0, 3.0]),),
        (Vectors.dense([4.0, 5.0, 6.0]),)]
df = spark.createDataFrame(data, ["vector"])

transformer = ElementwiseProduct(scalingVec=Vectors.dense([0.0, 1.0, 2.0]),
                                 inputCol="vector",
                                 outputCol="transformedVector")

# Batch transform the vectors to create new column:
transformer.transform(df).show()
