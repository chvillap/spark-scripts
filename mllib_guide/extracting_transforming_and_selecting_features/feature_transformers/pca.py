from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import PCA

# PCA is a statistical procedure that uses an orthogonal transformation to
# convert a set of observations of possibly correlated variables into a set
# of values of linearly uncorrelated variables called principal components.
# A PCA class trains a model to project vectors to a low-dimensional space
# using PCA. The example below shows how to project 5-dimensional feature
# vectors into 3-dimensional principal components.

spark = SparkSession.builder.appName("PCA").getOrCreate()

data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
        (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
        (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]

df = spark.createDataFrame(data, ["features"])

pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df)

result = model.transform(df).select("pcaFeatures")
result.show(truncate=False)

spark.stop()
