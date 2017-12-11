from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

spark = SparkSession.builder.appName("Correlation").getOrCreate()

# In spark.ml we provide the flexibility to calculate pairwise correlations
# among many series. The supported correlation methods are currently Pearson's
# and Spearman's correlation.

# Correlation computes the correlation matrix for the input Dataset of Vectors
# using the specified method. The output will be a DataFrame that contains the
# correlation matrix of the column of vectors.

data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df = spark.createDataFrame(data, ["features"])

r1 = Correlation.corr(df, "features").head()
print("Pearson correlation matrix:\n" + str(r1[0]))

r2 = Correlation.corr(df, "features", "spearman").head()
print("Spearman correlation matrix:\n" + str(r2[0]))

spark.stop()
