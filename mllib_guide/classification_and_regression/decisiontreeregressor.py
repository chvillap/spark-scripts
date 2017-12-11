from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Decision trees and their ensembles are popular methods for the machine
# learning tasks of classification and regression. Decision trees are widely
# used since they are easy to interpret, handle categorical features, extend
# to the multiclass classification setting, do not require feature scaling,
# and are able to capture non-linearities and feature interactions. Tree
# ensemble algorithms such as random forests and boosting are among the top
# performers for classification and regression tasks.

# The spark.ml implementation supports decision trees for binary and multiclass
# classification and for regression, using both continuous and categorical
# features. The implementation partitions data by rows, allowing distributed
# training with millions or even billions of instances.

spark = SparkSession.builder.appName("DecisionTreeRegressor").getOrCreate()

# Load the data stored in LIBSVM format as a DataFrame.
data = spark.read.format("libsvm").load("sample_libsvm_data.txt")

# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated
# as continuous.
featureIndexer = VectorIndexer(inputCol="features",
                               outputCol="indexedFeatures",
                               maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing).
trainingData, testData = data.randomSplit([0.7, 0.3])

# Instantiate a DecisionTree estimator.
dt = DecisionTreeRegressor(featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline.
pipeline = Pipeline(stages=[featureIndexer, dt])

# Train model. This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show()

# Select (prediction, true label) and compute test error.
evaluator = RegressionEvaluator(labelCol="label",
                                predictionCol="prediction",
                                metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

treeModel = model.stages[1]
print(treeModel)  # summary only
