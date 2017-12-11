from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import HashingTF
from pyspark.ml.feature import IDF

# HashingTF is a Transformer which takes sets of terms and converts those sets
# into fixed-length feature vectors.
# CountVectorizer converts text documents to vectors of term counts.
# HashingTF (and CountVectorizer) are Transforms, but IDF is an Estimator,
# which fits on a DataFrame and produces an IDFModel.
# The IDFModel takes feature vectors and scales each column. Intuitively, it
# down-weights columns which appear frequently in a corpus.

spark = SparkSession.builder.appName("TF-IDF").getOrCreate()

sentenceData = spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
], ["label", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures",
                      numFeatures=20)
featurizedData = hashingTF.transform(wordsData)

# Alternatively, CountVectorizer can be used to get term frequency vectors too.

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rescaledData.select("label", "features").show(truncate=False)

spark.stop()
