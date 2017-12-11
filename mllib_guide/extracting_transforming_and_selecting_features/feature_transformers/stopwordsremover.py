from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover

# Stop words are words which should be excluded from the input, typically
# because the words appear frequently and don’t carry as much meaning.

# StopWordsRemover takes as input a sequence of strings (e.g. the output of a
# Tokenizer) and drops all the stop words from the input sequences. The list of
# stopwords is specified by the stopWords parameter. Default stop words for
# some languages are accessible by calling
# StopWordsRemover.loadDefaultStopWords(language), for which available options
# are "danish", "dutch", "english", "finnish", "french", "german", "hungarian",
# "italian", "norwegian", "portuguese", "russian", "spanish", "swedish" and
# "turkish". A boolean parameter caseSensitive indicates if the matches should
# be case sensitive (false by default).

spark = SparkSession.builder.appName("StopWordsRemover").getOrCreate()

sentenceData = spark.createDataFrame([
    (0, ["I", "saw", "the", "red", "balloon"]),
    (1, ["Mary", "had", "a", "little", "lamb"])
], ["id", "raw"])

remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
remover.transform(sentenceData).show(truncate=False)

spark.stop()
