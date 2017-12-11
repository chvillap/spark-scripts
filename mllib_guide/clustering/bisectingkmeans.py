from pyspark.sql import SparkSession
from pyspark.ml.clustering import BisectingKMeans

# Bisecting K-means can often be much faster than regular K-means, but it will
# generally produce a different clustering.
# Bisecting k-means is a kind of hierarchical clustering. Hierarchical
# clustering is one of the most commonly used method of cluster analysis which
# seeks to build a hierarchy of clusters. Strategies for hierarchical
# clustering generally fall into two types:
#   - Agglomerative: This is a "bottom up" approach: each observation starts in
#     its own cluster, and pairs of clusters are merged as one moves up the
#     hierarchy.
#   - Divisive: This is a "top down" approach: all observations start in one
#     cluster, and splits are performed recursively as one moves down the
#     hierarchy.

# Bisecting k-means algorithm is a kind of divisive algorithms. The
# implementation in MLlib has the following parameters:
#   - k: the desired number of leaf clusters (default: 4). The actual number
#     could be smaller if there are no divisible leaf clusters.
#   - maxIterations: the max number of k-means iterations to split clusters
#     (default: 20).
#   - minDivisibleClusterSize: the minimum number of points (if >= 1.0) or the
#     minimum proportion of points (if < 1.0) of a divisible cluster
#     (default: 1).
#   - seed: a random seed (default: hash value of the class name).

spark = SparkSession.builder.appName("BisectingKMeans").getOrCreate()

# Load data.
dataset = spark.read.format("libsvm") \
                    .load("sample_kmeans_data.txt") \
                    .toDF("id", "features")

# Train a bisecting K-means model.
bkm = BisectingKMeans().setK(2).setSeed(1)
model = bkm.fit(dataset)

# Evaluate clustering.
cost = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(cost))

# Show the result.
centers = model.clusterCenters()
print("Cluster centers:")
for center in centers:
    print(center)

clustered = model.transform(dataset)
clustered.show()

spark.stop()
