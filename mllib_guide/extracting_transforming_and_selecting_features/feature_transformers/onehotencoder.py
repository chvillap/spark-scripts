from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder

# One-hot encoding maps a column of label indices to a column of binary
# vectors, with at most a single one-value. This encoding allows algorithms
# which expect continuous features, such as Logistic Regression, to use
# categorical features.

spark = SparkSession.builder.appName("OneHotEncoder").getOrCreate()

df = spark.createDataFrame([
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
], ["id", "category"])

stringIndexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = stringIndexer.fit(df)
indexed = model.transform(df)

encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
encoded = encoder.transform(indexed)
encoded.show()

spark.stop()
