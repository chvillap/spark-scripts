from pyspark.sql import SparkSession
from pyspark.ml.regression import GeneralizedLinearRegression

# Contrasted with linear regression where the output is assumed to follow a
# Gaussian distribution, generalized linear models (GLMs) are specifications
# of linear models where the response variable Yi follows some distribution
# from the exponential family of distributions. Spark's
# GeneralizedLinearRegression interface allows for flexible specification of
# GLMs which can be used for various types of prediction problems including
# linear regression, Poisson regression, logistic regression, and others.
# Currently in spark.ml, only a subset of the exponential family distributions
# are supported, and they are listed below.
#
# Family     Response Type   Supported Links
# --------------------------------------------------
# Gaussian   Continuous      Identity*, Log, Inverse
# Binomial   Binary          Logit*, Probit, CLogLog
# Poisson    Count           Log*, Identity, Sqrt
# Gamma      Continuous      Inverse*, Idenity, Log
#
# * Canonical Link

spark = SparkSession.builder \
                    .appName("GeneralizedLinearRegression") \
                    .getOrCreate()

# Load training data.
dataset = spark.read \
               .format("libsvm") \
               .load("sample_linear_regression_data.txt")

glr = GeneralizedLinearRegression(family="gaussian", link="identity",
                                  maxIter=10, regParam=0.3)

# Fit the model.
model = glr.fit(dataset)

# Print the coefficients and intercept for generalized linear regression model.
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))

# Summarize the model over the training set and print out some metrics.
summary = model.summary
print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
print("T Values: " + str(summary.tValues))
print("P Values: " + str(summary.pValues))
print("Dispersion: " + str(summary.dispersion))
print("Null Deviance: " + str(summary.nullDeviance))
print("Residual Degree of Freedom Null: " +
      str(summary.residualDegreeOfFreedomNull))
print("Deviance: " + str(summary.deviance))
print("Residual Degree of Freedom: " + str(summary.residualDegreeOfFreedom))
print("AIC: " + str(summary.aic))
print("Deviance Residuals:")
summary.residuals().show()
