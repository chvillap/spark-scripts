from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorIndexer

# VectorIndexer helps index categorical features in datasets of Vectors. It can
# both automatically decide which features are categorical and convert original
# values to category indices. Specifically, it does the following:
#    1. Take an input column of type Vector and a parameter maxCategories.
#    2. Decide which features should be categorical based on the number of
#       distinct values, where features with at most maxCategories are declared
#       categorical.
#    3. Compute 0-based category indices for each categorical feature.
#    4. Index categorical features and transform original feature values to
#       indices.

# Indexing categorical features allows algorithms such as Decision Trees and
# Tree Ensembles to treat categorical features appropriately, improving
# performance.

spark = SparkSession.builder.appName("VectorIndexer").getOrCreate()

data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
data.show()

indexer = VectorIndexer(inputCol="features", outputCol="indexed",
                        maxCategories=10)
indexerModel = indexer.fit(data)

categoricalFeatures = indexerModel.categoryMaps
print("Chose %d categorical features: %s" %
      (len(categoricalFeatures),
       ", ".join([str(k) for k in categoricalFeatures.keys()])))

# Create new column "indexed" with categorical values transformed to indices.
indexedData = indexerModel.transform(data)
indexedData.show()

spark.stop()
