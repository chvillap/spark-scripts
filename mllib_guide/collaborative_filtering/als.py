from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Collaborative filtering is commonly used for recommender systems. These
# techniques aim to fill in the missing entries of a user-item association
# matrix. spark.ml currently supports model-based collaborative filtering,
# in which users and products are described by a small set of latent factors
# that can be used to predict missing entries. spark.ml uses the alternating
# least squares (ALS) algorithm to learn these latent factors. The
# implementation in spark.ml has the following parameters:
#   - numBlocks is the number of blocks the users and items will be
#     partitioned into in order to parallelize computation (defaults to 10).
#   - rank is the number of latent factors in the model (defaults to 10).
#   - maxIter is the maximum number of iterations to run (defaults to 10).
#   - regParam specifies the regularization parameter in ALS (defaults to 1.0).
#   - implicitPrefs specifies whether to use the explicit feedback ALS variant
#     or one adapted for implicit feedback data (defaults to false which means
#     using explicit feedback).
#   - alpha is a parameter applicable to the implicit feedback variant of ALS
#     that governs the baseline confidence in preference observations (defaults
#     to 1.0).
#   - nonnegative specifies whether or not to use nonnegative constraints for
#     least squares (defaults to false).

# The standard approach to matrix factorization based collaborative filtering
# treats the entries in the user-item matrix as explicit preferences given by
# the user to the item, for example, users giving ratings to movies.
#
# It is common in many real-world use cases to only have access to implicit
# feedback (e.g. views, clicks, purchases, likes, shares etc.). Essentially,
# instead of trying to model the matrix of ratings directly, this approach
# treats the data as numbers representing the strength in observations of user
# actions (such as the number of clicks, or the cumulative duration someone
# spent viewing a movie). Those numbers are then related to the level of
# confidence in observed user preferences, rather than explicit ratings given
# to items. The model then tries to find latent factors that can be used to
# predict the expected preference of a user for an item.

# We scale the regularization parameter regParam in solving each least squares
# problem by the number of ratings the user generated in updating user factors,
# or the number of ratings the product received in updating product factors.
# This approach is named "ALS-WR". It makes regParam less dependent on the
# scale of the dataset, so we can apply the best parameter learned from a
# sampled subset to the full dataset and expect similar performance.

spark = SparkSession.builder.appName("ALS").getOrCreate()

lines = spark.read.text("sample_movielens_ratings.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]),
                                     rating=float(p[2]), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratingsRDD)
training, test = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data.
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating",
          maxIter=5, regParam=0.01)
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data.
predictions = model.transform(test)
predictions.show(truncate=False)

evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction",
                                metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Square Error = " + str(rmse))

spark.stop()

# If the rating matrix is derived from another source of information (i.e. it
# is inferred from other signals), you can set implicitPrefs to True to get
# better results.
