# Spark 1.4 also added a suite of mathematical functions. The inputs need to
# be columns functions that take a single argument, such as cos, sin, floor,
# ceil. For functions that take two arguments as input, such as pow, hypot,
# either two columns or a combination of a double and column can be supplied.

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Summary and descriptive statistics") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

df = sqlContext.range(0, 10).withColumn('uniform', rand(seed=10) * 3.14)

# You can reference a column or supply the column name.
df.select(
    'uniform',
    toDegrees('uniform'),
    (pow(cos(df['uniform']), 2) + pow(sin(df.uniform), 2))
    .alias("cos^2 + sin^2")).show()

spark.stop()
