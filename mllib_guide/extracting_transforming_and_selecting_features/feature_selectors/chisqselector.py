from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import ChiSqSelector

# ChiSqSelector stands for Chi-Squared feature selection. It operates on
# labeled data with categorical features. ChiSqSelector uses the Chi-Squared
# test of independence to decide which features to choose. It supports three
# selection methods: numTopFeatures, percentile, fpr:
#   - numTopFeatures chooses a fixed number of top features according to a
#     chi-squared test. This is akin to yielding the features with the most
#     predictive power.
#   - percentile is similar to numTopFeatures but chooses a fraction of all
#     features instead of a fixed number.
#   - fpr chooses all features whose p-value is below a threshold, thus
#     controlling the false positive rate of selection.

# By default, the selection method is numTopFeatures, with the default number
# of top features set to 50. The user can choose a selection method using
# setSelectorType.

spark = SparkSession.builder.appName("ChiSqSelector").getOrCreate()

df = spark.createDataFrame(
    [(7, Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0,),
     (8, Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0,),
     (9, Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0,)],
    ["id", "features", "clicked"])

selector = ChiSqSelector(numTopFeatures=1, featuresCol="features",
                         outputCol="selectedFeatures", labelCol="clicked")

model = selector.fit(df)
result = model.transform(df)

print("ChiSqSelector output with top %d features selected" %
      selector.getNumTopFeatures())
result.show()

spark.stop()
