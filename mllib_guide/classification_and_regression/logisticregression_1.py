from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression

# Logistic regression is a popular method to predict a categorical response. It
# is a special case of Generalized Linear models that predicts the probability
# of the outcomes. In spark.ml logistic regression can be used to predict a
# binary outcome by using binomial logistic regression, or it can be used to
# predict a multiclass outcome by using multinomial logistic regression. Use
# the "family" parameter to select between these two algorithms, or leave it
# unset and Spark will infer the correct variant.

# regParam (lambda): defines the trade-off between the two goals of minimizing
# the loss (i.e., training error) and minimizing model complexity (i.e., to
# avoid overfitting). Greater values mean more regularization.
#
# elasticNetParam (alpha): trade-off between L1 and L2 regularization, when
# using regularization. Greater give more weight to L1. Only supported in
# binary logistic regression.

spark = SparkSession.builder.appName("LogisticRegression 1").getOrCreate()

# Load training data.
training = spark.read.format("libsvm").load("sample_libsvm_data.txt")

# Fit the model.
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel = lr.fit(training)

# Print the coefficients and intercept for logistic regression.
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))

# # THIS DOES NOT WORK IN SPARK 2.0.2!
#
# # We can also use the multinomial family for binary classification.
# mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8,
#                          family="multinomial")
#
# # Fit the model.
# mlrModel = mlr.fit(training)
#
# # Print the coefficients and intercepts for logistic regression with
# # multinomial family.
# print("Coefficients: " + str(mlrModel.coefficientMatrix))
# print("Intercept: " + str(mlrModel.interceptVector))

# LogisticRegressionTrainingSummary provides a summary for a
# LogisticRegressionModel. Currently, only binary classification is supported.
# Support for multiclass model summaries will be added in the future.

# Extract the summary from the returned LogisticRegressionModel instance.
trainingSummary = lrModel.summary

# Obtain the objective per iteration.
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
    print(objective)

# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# Set the model threshold to maximize F-Measure.
fMeasure = trainingSummary.fMeasureByThreshold
maxFMeasure = fMeasure.groupBy() \
                      .max("F-Measure") \
                      .select("max(F-Measure)") \
                      .head()
bestThreshold = fMeasure \
    .where(fMeasure["F-Measure"] == maxFMeasure["max(F-Measure)"]) \
    .select("threshold") \
    .head()["threshold"]
lr.setThreshold(bestThreshold)

print("Best threshold: " + str(bestThreshold))

spark.stop()
