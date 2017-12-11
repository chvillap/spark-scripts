from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer

# StringIndexer encodes a string column of labels to a column of label indices.
# The indices are in [0, numLabels), ordered by label frequencies, so the most
# frequent label gets index 0. If the input column is numeric, we cast it to
# string and index the string values. When downstream pipeline components such
# as Estimator or Transformer make use of this string-indexed label, you must
# set the input column of the component to this string-indexed column name. In
# many cases, you can set the input column with setInputCol.

# Additionally, there are two strategies regarding how StringIndexer will
# handle unseen labels when you have fit a StringIndexer on one dataset and
# then use it to transform another:
#   * throw an exception (which is the default)
#   * skip the row containing the unseen label entirely

spark = SparkSession.builder.appName("StringIndexer").getOrCreate()

df = spark.createDataFrame([
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
], ["id", "category"])

indexer = StringIndexer(inputCol="category", outputCol="categoryIndexer")
model = indexer.fit(df)

indexed = model.transform(df)
indexed.show()

spark.stop()
