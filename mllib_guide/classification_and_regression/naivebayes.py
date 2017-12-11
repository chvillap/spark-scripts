from pyspark.sql import SparkSession
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Naive Bayes is a simple multiclass classification algorithm with the
# assumption of independence between every pair of features. Naive Bayes can
# be trained very efficiently. Within a single pass to the training data, it
# computes the conditional probability distribution of each feature given
# label, and then it applies Bayes' theorem to compute the conditional
# probability distribution of label given an observation and use it for
# prediction.

# spark.ml supports multinomial Naive Bayes and Bernoulli Naive Bayes. These
# models are typically used for document classification. Within that context,
# each observation is a document and each feature represents a term whose value
# is the frequency of the term (in multinomial Naive Bayes) or a zero or one
# indicating whether the term was found in the document (in Bernoulli naive
# Bayes). Feature values must be nonnegative. The model type is selected with
# an optional parameter "multinomial" or "bernoulli" with "multinomial" as the
# default. Additive smoothing can be used by setting the parameter λ (default
# to 1.0). For document classification, the input feature vectors are usually
# sparse, and sparse vectors should be supplied as input to take advantage of
# sparsity.

spark = SparkSession.builder.appName("NaiveBayes").getOrCreate()

data = spark.read.format("libsvm").load("sample_libsvm_data.txt")

# Split the data into train and test.
train, test = data.randomSplit([0.6, 0.4], 1234)

# Create the trainer and set its parameters.
mnb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# Train the model.
model = mnb.fit(train)

# Make predictions.
predictions = model.transform(test)
predictions.show()

# Compute accuracy on the test set.
evaluator = MulticlassClassificationEvaluator(labelCol="label",
                                              predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print("Test set accuracy = " + str(accuracy))

spark.stop()
