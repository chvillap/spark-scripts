from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression

# Multiclass classification is supported via multinomial logistic (softmax)
# regression. In multinomial logistic regression, the algorithm produces K sets
# of coefficients, or a matrix of dimension K×J where K is the number of
# outcome classes and J is the number of features. If the algorithm is fit with
# an intercept term then a length K vector of intercepts is available.

# Multinomial coefficients are available as coefficientMatrix and intercepts
# are available as interceptVector.

# coefficients and intercept methods on a logistic regression model trained
# with multinomial family are not supported. Use coefficientMatrix and
# interceptVector instead.

spark = SparkSession.builder.appName("LogisticRegression 2").getOrCreate()

# Load training data.
training = spark.read \
                .format("libsvm") \
                .load("sample_multiclass_classification_data.txt")

# # THIS DOES NOT WORK IN SPARK 2.0.2!
#
# # Currently, LogisticRegression with ElasticNet in ML package only support
# # binary classsification.
# lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# # Fit the model.
# lrModel = lr.fit(training)

# # Print the coefficients and intercept for multinomial logistic regression.
# print("Coefficients:\n" + str(lrModel.coefficientMatrix))
# print("Intercept: " + str(lrModel.interceptVector))

spark.stop()
