from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest

spark = SparkSession.builder.appName("Correlation").getOrCreate()

# Hypothesis testing is a powerful tool in statistics to determine whether a
# result is statistically significant, whether this result occurred by chance
# or not. spark.ml currently supports Pearson’s Chi-squared  tests for
# independence.

# ChiSquareTest conducts Pearson’s independence test for every feature against
# the label. For each feature, the (feature, label) pairs are converted into a
# contingency matrix for which the Chi-squared statistic is computed. All
# label and feature values must be categorical.

data = [(0.0, Vectors.dense(0.5, 10.0)),
        (0.0, Vectors.dense(1.5, 20.0)),
        (1.0, Vectors.dense(1.5, 30.0)),
        (0.0, Vectors.dense(3.5, 30.0)),
        (0.0, Vectors.dense(3.5, 40.0)),
        (1.0, Vectors.dense(3.5, 40.0))]
df = spark.createDataFrame(data, ["label", "features"])

r = ChiSquareTest.test(df, "features", "label").head()

print("pValues: " + str(r.pValues))
print("degreesOfFreedom: " + str(r.degreesOfFreedom))
print("statistics: " + str(r.statistics))

spark.stop()
