# Cross Tabulation provides a table of the frequency distribution for a set
# of variables. Cross-tabulation is a powerful tool in statistics that is
# used to observe the statistical significance (or independence) of variables.
# Here is an example on how to use crosstab to obtain the contingency table.

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("Summary and descriptive statistics") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

# Create a DataFrame with two columns (name, item).
names = ["Alice", "Bob", "Mike"]
items = ["milk", "bread", "butter", "apples", "oranges"]
df = sqlContext.createDataFrame(
    [(names[i % 3], items[i % 5]) for i in range(100)],
    ["name", "item"])

df.show(10)

# Cross-tabulate two columns of a DataFrame in order to obtain the counts of
# the different pairs that are observed in those columns.
df.stat.crosstab("name", "item").show()

# One important thing to keep in mind is that the cardinality of columns we
# run crosstab on cannot be too big. That is to say, the number of distinct
# "name" and "item" cannot be too large. Just imagine if "item" contains 1
# billion distinct entries: how would you fit that table on your screen?!

spark.stop()
