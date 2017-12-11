from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import PolynomialExpansion

# Polynomial expansion is the process of expanding your features into a
# polynomial space, which is formulated by an n-degree combination of original
# dimensions. A PolynomialExpansion class provides this functionality.

spark = SparkSession.builder.appName("PolynomialExpansion").getOrCreate()

df = spark.createDataFrame([
    (Vectors.dense([2.0, 1.0]),),
    (Vectors.dense([0.0, 0.0]),),
    (Vectors.dense([3.0, -1.0]),)
], ["features"])

polyExpansion = PolynomialExpansion(inputCol="features",
                                    outputCol="polyFeatures",
                                    degree=3)
polyDF = polyExpansion.transform(df)
polyDF.show(truncate=False)

spark.stop()
