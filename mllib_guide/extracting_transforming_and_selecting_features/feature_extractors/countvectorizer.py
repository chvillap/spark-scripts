from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer

# CountVectorizer and CountVectorizerModel aim to help convert a collection of
# text documents to vectors of token counts. The model produces sparse
# representations for the documents over the vocabulary, which can then be
# passed to other algorithms like LDA.

# During the fitting process, CountVectorizer will select the top vocabSize
# words ordered by term frequency across the corpus. An optional parameter
# minDF also affects the fitting process by specifying the minimum number
# (or fraction if < 1.0) of documents a term must appear in to be included in
# the vocabulary. Another optional binary toggle parameter controls the output
# vector. If set to true all nonzero counts are set to 1. This is especially
# useful for discrete probabilistic models that model binary, rather than
# integer, counts.

spark = SparkSession.builder.appName("CountVectorizer").getOrCreate()

# Input data: Each row is a bag of words with a ID.
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
], ["id", "words"])

# Fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="words", outputCol="features",
                     vocabSize=3, minDF=2.0)
model = cv.fit(df)

result = model.transform(df)
result.show(truncate=False)

spark.stop()
