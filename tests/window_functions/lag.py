from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from random import randint

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

df = spark.createDataFrame([
    {'date': '2017-11-30', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-11-30', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-11-30', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-11-30', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-11-15', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-11-15', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-11-15', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-11-15', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-10-31', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-10-31', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-10-31', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-10-31', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-10-15', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-10-15', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-10-15', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-10-15', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-09-30', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-09-30', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-09-30', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-09-30', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-09-15', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-09-15', 'user': 'carlos', 'value': randint(0, 9)},
    {'date': '2017-09-15', 'user': 'henrique', 'value': randint(0, 9)},
    {'date': '2017-09-15', 'user': 'henrique', 'value': randint(0, 9)},
])
df.show()

df2 = df.groupBy('date', 'user') \
        .avg('value') \
        .orderBy('date', 'user') \
        .withColumnRenamed('avg(value)', 'avg_value')
df2.show()

w = Window.partitionBy('user').orderBy('date')
df2.withColumn('avg_value', F.lag('avg_value', count=1, default=0).over(w)) \
   .show()

spark.stop()
