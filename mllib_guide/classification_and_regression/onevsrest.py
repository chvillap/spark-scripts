from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# OneVsRest is an example of a machine learning reduction for performing
# multiclass classification given a base classifier that can perform binary
# classification efficiently. It is also known as "One-vs-All".

# OneVsRest is implemented as an Estimator. For the base classifier it takes
# instances of Classifier and creates a binary classification problem for each
# of the k classes. The classifier for class i is trained to predict whether
# the label is i or not, distinguishing class i from all other classes.

# Predictions are done by evaluating each binary classifier and the index of
# the most confident classifier is output as label.

spark = SparkSession.builder.appName("OneVsRest").getOrCreate()

# Load data file.
inputData = spark.read \
                 .format("libsvm") \
                 .load("sample_multiclass_classification_data.txt")

# Generate the train/test split.
train, test = inputData.randomSplit([0.8, 0.2])

# Instantiate the base classifier.
lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)

# Instantiate the One Vs Rest Classifier.
ovr = OneVsRest(classifier=lr)

# Train the multiclass model.
ovrModel = ovr.fit(train)

# Score the model on test data.
predictions = ovrModel.transform(test)

# Obtain evaluator.
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

# Compute the classification error on test data.
accuracy = evaluator.evaluate(predictions)

print("Test Error = %g" % (1.0 - accuracy))

spark.stop()
