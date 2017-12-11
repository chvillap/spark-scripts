from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# In addition to CrossValidator, Spark also offers TrainValidationSplit for
# hyper-parameter tuning. TrainValidationSplit only evaluates each combination
# of parameters once, as opposed to k times in the case of CrossValidator. It
# is therefore less expensive, but will not produce as reliable results when
# the training dataset is not sufficiently large.

# Unlike CrossValidator, TrainValidationSplit creates a single (training, test)
# dataset pair. It splits the dataset into these two parts using the trainRatio
# parameter. For example with trainRatio=0.75, TrainValidationSplit will
# generate a training and test dataset pair where 75% of the data is used for
# training and 25% for validation.

# Like CrossValidator, TrainValidationSplit finally fits the Estimator using
# the best ParamMap and the entire dataset.

spark = SparkSession.builder.appName("Train-Validation Split").getOrCreate()

# Prepare training and test data.
data = spark.read.format("libsvm").load("sample_linear_regression_data.txt")
train, test = data.randomSplit([0.9, 0.1], seed=12345)

lr = LinearRegression(maxIter=10)

# We use a ParamGridBuilder to construct a grid of parameters to search over.
# TrainValidationSplit will try all combinations of values and determine best
# model using the evaluator.
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1]) \
    .addGrid(lr.fitIntercept, [False, True]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

# In this case the estimator is simply the linear regression.
# A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps,
# and an Evaluator.
tvs = TrainValidationSplit(estimator=lr,
                           estimatorParamMaps=paramGrid,
                           evaluator=RegressionEvaluator(),
                           trainRatio=0.8)
# 80% of the data will be used for training, and 20% for validation.

# Run TrainValidationSplit, and choose the best set of parameters.
tvsModel = tvs.fit(train)

# Make predictions on test data. tvsModel is the model with combination of
# parameters that performed best.
prediction = tvsModel.transform(test)
prediction.select("features", "label", "prediction").show()

spark.stop()
