from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec

# Word2Vec is an Estimator which takes sequences of words representing
# documents and trains a Word2VecModel. The model maps each word to a unique
# fixed-size vector. The Word2VecModel transforms each document into a vector
# using the average of all words in the document; this vector can then be used
# as features for prediction, document similarity calculations, etc.

spark = SparkSession.builder.appName("Word2Vec").getOrCreate()

# Input data: Each row is a bag of words from a sentence or document.
documentDF = spark.createDataFrame([
    ("Hi I heard about Spark".split(" "), ),
    ("I wish Java could use case classes".split(" "), ),
    ("Logistic regression models are neat".split(" "), )
], ["text"])

# Learn a mapping from words to Vectors.
word2Vec = Word2Vec(vectorSize=3, minCount=0,
                    inputCol="text", outputCol="result")
model = word2Vec.fit(documentDF)

result = model.transform(documentDF)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))

spark.stop()
