from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

# The interface for working with linear regression models and model summaries
# is similar to the logistic regression case.

spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

# Load training data.
training = spark.read \
                .format("libsvm") \
                .load("sample_linear_regression_data.txt")

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model.
lrModel = lr.fit(training)

# Print the coefficients and intercept for linear regression.
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics.
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("R^2: %f" % trainingSummary.r2)

spark.stop()
