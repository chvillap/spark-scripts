from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Gradient-Boosted Trees (GBTs) are ensembles of decision trees. GBTs
# iteratively train decision trees in order to minimize a loss function. The
# spark.ml implementation supports GBTs for binary classification and for
# regression, using both continuous and categorical features.

spark = SparkSession.builder.appName("GBTRegressor").getOrCreate()

# Load and parse the data file, converting it to a DataFrame.
data = spark.read.format("libsvm").load("sample_libsvm_data.txt")

# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as
# continuous.
featureIndexer = VectorIndexer(inputCol="features",
                               outputCol="indexedFeatures",
                               maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing).
trainingData, testData = data.randomSplit([0.7, 0.3])

gbt = GBTRegressor(featuresCol="indexedFeatures", maxIter=10)

# Chain indexers and GBT in a Pipeline.
pipeline = Pipeline(stages=[featureIndexer, gbt])

# Train model. This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show()

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(labelCol="label",
                                predictionCol="prediction",
                                metricName="rmse")
rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

gbtModel = model.stages[1]
print(gbtModel)  # summary only

spark.stop()
