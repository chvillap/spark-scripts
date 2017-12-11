from pyspark.sql import SparkSession
from pyspark.ml.feature import NGram

# An n-gram is a sequence of n tokens (typically words) for some integer n.
# The NGram class can be used to transform input features into n-grams.

# NGram takes as input a sequence of strings (e.g. the output of a Tokenizer).
# The parameter n is used to determine the number of terms in each n-gram. The
# output will consist of a sequence of n-grams where each n-gram is represented
# by a space-delimited string of $n$ consecutive words. If the input sequence
#  contains fewer than n strings, no output is produced.

spark = SparkSession.builder.appName("NGram").getOrCreate()

wordDataFrame = spark.createDataFrame([
    (0, ["Hi", "I", "heard", "about", "Spark"]),
    (1, ["I", "wish", "Java", "could", "use", "case", "classes"]),
    (2, ["Logistic", "regression", "models", "are", "neat"])
], ["id", "words"])

ngram = NGram(n=2, inputCol="words", outputCol="ngrams")

ngramDataFrame = ngram.transform(wordDataFrame)
ngramDataFrame.select("ngrams").show(truncate=False)

spark.stop()
