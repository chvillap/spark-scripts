from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler

# StandardScaler transforms a dataset of Vector rows, normalizing each feature
# to have unit standard deviation and/or zero mean. It takes parameters:
#    withStd: True by default. Scales the data to unit standard deviation.
#    withMean: False by default. Centers the data with mean before scaling.
# It will build a dense output, so take care when applying to sparse input.

# StandardScaler is an Estimator which can be fit on a dataset to produce a
# StandardScalerModel; this amounts to computing summary statistics. The model
# can then transform a Vector column in a dataset to have unit standard
# deviation and/or zero mean features.

# Note that if the standard deviation of a feature is zero, it will return
# default 0.0 value in the Vector for that feature.

spark = SparkSession.builder.appName("StandardScaler").getOrCreate()

dataFrame = spark.read.format("libsvm").load("sample_libsvm_data.txt")
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                        withStd=True, withMean=False)

# Compute summary statistics by fitting the StandardScaler.
scalerModel = scaler.fit(dataFrame)

# Normalize each feature to have unit standard deviation.
scaledData = scalerModel.transform(dataFrame)
scaledData.show()

spark.stop()
