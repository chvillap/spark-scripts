from pyspark.sql import SparkSession
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Multilayer perceptron classifier (MLPC) is a classifier based on the
# feedforward artificial neural network. MLPC consists of multiple layers of
# nodes. Each layer is fully connected to the next layer in the network. Nodes
# in the input layer represent the input data. All other nodes map inputs to
# outputs by a linear combination of the inputs with the node’s weights w and
# bias b and applying an activation function.

# Nodes in intermediate layers use sigmoid (logistic) function.
# Nodes in the output layer use softmax function.

# The number of nodes N in the output layer corresponds to the number of
# classes.

# MLPC employs backpropagation for learning the model. We use the logistic
# loss function for optimization and L-BFGS as an optimization routine.

spark = SparkSession.builder \
                    .appName("MultilayerPerceptronClassifier") \
                    .getOrCreate()

# Load training data.
data = spark.read \
            .format("libsvm") \
            .load("sample_multiclass_classification_data.txt")

# Split the data into train and test.
train, test = data.randomSplit([0.6, 0.4], 1234)

# Specify layers for the neural network:
# Input layer of size 4 (features), two intermediate layers of sizes 5 and 4,
# and output layer of size 3 (classes).
layers = [4, 5, 4, 3]

# create the trainer and set its parameters.
trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers,
                                         blockSize=128, seed=1234)

# Train the model.
model = trainer.fit(train)

# Compute accuracy on the test set.
result = model.transform(test)
predictionsAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

print("Test set accuracy = " + str(evaluator.evaluate(predictionsAndLabels)))

spark.stop()
