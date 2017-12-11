from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import AFTSurvivalRegression

# In spark.ml, we implement the Accelerated failure time (AFT) model which is
# a parametric survival regression model for censored data. It describes a
# model for the log of survival time, so it's often called a log-linear model
# for survival analysis. Different from a Proportional hazards model designed
# for the same purpose, the AFT model is easier to parallelize because each
# instance contributes to the objective function independently.

# The most commonly used AFT model is based on the Weibull distribution of the
# survival time. The Weibull distribution for lifetime corresponds to the
# extreme value distribution for the log of the lifetime.

spark = SparkSession.builder.appName("AFTSurvivalRegression").getOrCreate()

training = spark.createDataFrame([
    (1.218, 1.0, Vectors.dense(1.560, -0.605)),
    (2.949, 0.0, Vectors.dense(0.346, 2.158)),
    (3.627, 0.0, Vectors.dense(1.380, 0.231)),
    (0.273, 1.0, Vectors.dense(0.520, 1.151)),
    (4.199, 0.0, Vectors.dense(0.795, -0.226))
], ["label", "censor", "features"])

quantileProbabilities = [0.3, 0.6]
aft = AFTSurvivalRegression(quantilesCol="quantiles",
                            quantileProbabilities=quantileProbabilities)

model = aft.fit(training)

# Print the coefficients, intercept and scale parameter for AFT survival
# regression.
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))
print("Scale: " + str(model.scale))
model.transform(training).show(truncate=False)
