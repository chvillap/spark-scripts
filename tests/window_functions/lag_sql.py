from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from random import randint

spark = SparkSession.builder.appName("Window Functions").getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

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

df.createOrReplaceTempView('temp')

df2 = sqlContext.sql('''
    SELECT date,
           user,
           AVG(value) as avg_value
    FROM temp
    GROUP BY date, user
    ORDER BY date, user
''')
df2.show()

df2.createOrReplaceTempView('temp2')

df3 = sqlContext.sql('''
    SELECT date,
           user,
           LAG(avg_value, 1)
               OVER (PARTITION BY user ORDER BY date) as prev,
           avg_value as current,
           LEAD(avg_value, 1)
               OVER (PARTITION BY user ORDER BY date) as next
    FROM temp2
''')
df3.show()

spark.catalog.dropTempView('temp')
spark.catalog.dropTempView('temp2')

spark.stop()
