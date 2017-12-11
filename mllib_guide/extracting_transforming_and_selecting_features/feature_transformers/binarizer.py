from pyspark.sql import SparkSession
from pyspark.ml.feature import Binarizer

# Binarization is the process of thresholding numerical features to binary
# (0/1) features.
# Binarizer takes the common parameters inputCol and outputCol, as well as the
# threshold for binarization. Feature values greater than the threshold are
# binarized to 1.0; values equal to or less than the threshold are binarized to
# 0.0. Both Vector and Double types are supported for inputCol.

spark = SparkSession.builder.appName("Binarizer").getOrCreate()

continuousDataFrame = spark.createDataFrame([
    (0, 0.1),
    (1, 0.8),
    (2, 0.2)
], ["id", "feature"])

binarizer = Binarizer(inputCol="feature", outputCol="binarized_feature",
                      threshold=0.5)
binarizedDataFrame = binarizer.transform(continuousDataFrame)

print("Binarized output with threshold = %f" % binarizer.getThreshold())
binarizedDataFrame.show()

spark.stop()
