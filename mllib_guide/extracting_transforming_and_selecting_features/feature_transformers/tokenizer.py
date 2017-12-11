from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import RegexTokenizer

# Tokenization is the process of taking text (such as a sentence) and breaking
# it into individual terms (usually words). A simple Tokenizer class provides
# this functionality.

# RegexTokenizer allows more advanced tokenization based on regular expression
# (regex) matching. By default, the parameter "pattern" (default: "\\s+") is
# used as delimiters to split the input text. Alternatively, users can set
#  parameter "gaps" to false indicating the regex "pattern" denotes "tokens"
# rather than splitting gaps, and find all matching occurrences as the
# tokenization result.

spark = SparkSession.builder.appName("Tokenizer").getOrCreate()

sentenceDataFrame = spark.createDataFrame([
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic,regression,models,are,neat")
], ["id", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")

regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words",
                                pattern="\\W")
# alternatively, pattern="\\w+", gaps(False)

# Does 'udf' mean 'user defined function'?
# 'col' seems to be a column selector (there is a 'row' too)

countTokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("sentence", "words") \
         .withColumn("tokens", countTokens(col("words"))) \
         .show(truncate=False)

regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words") \
              .withColumn("tokens", countTokens(col("words"))) \
              .show(truncate=False)

spark.stop()
