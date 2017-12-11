from pyspark.sql import SparkSession
from pyspark.ml.regression import IsotonicRegression

# Isotonic regression belongs to the family of regression algorithms. Formally
# isotonic regression is a problem where given a finite set of real numbers
# Y = {y_1, y_2, ..., y_n} representing observed responses and
# X = {x_1, x_2, ..., x_n} the unknown response values to be fitted finding a
# function that minimises f(x) = sum(w_i * (y_i - x_i)^2) with respect to
# complete order subject to x_1 <= x_2 <= ... <= x_n where w_i are positive
# weights. The resulting function is called isotonic regression and it is
# unique. It can be viewed as least squares problem under order restriction.
# Essentially isotonic regression is a monotonic function best fitting the
# original data points.

spark = SparkSession.builder.appName("IsotonicRegression").getOrCreate()

# Loads data.
dataset = spark.read \
               .format("libsvm") \
               .load("sample_isotonic_regression_libsvm_data.txt")
dataset.show()

# Trains an isotonic regression model.
model = IsotonicRegression().fit(dataset)

print("Boundaries in increasing order: %s\n" % str(model.boundaries))
print("Predictions associated with the boundaries: %s\n" %
      str(model.predictions))

# Makes predictions.
model.transform(dataset).show()

spark.stop()
