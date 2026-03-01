# PART 1

# https://numpy.org/doc/2.1/reference/random/generated/numpy.random.uniform.html

import numpy as np
import pandas as pd
np.random.seed(42) #for reproducibility

# a dataset of at least 6,000 points
pointsNum = 6000
k = 3

# Define cluster centers
centers = np.array([
    [2000, 200000, 2000, 200000],
    [4000, 400000, 4000, 400000],
    [6000, 600000, 6000, 600000],
])

# Standard deviations for each feature
std_dev = np.array([50, 5000, 50, 5000])

# Generate points
data_list = []
points_per_cluster = pointsNum // len(centers)
for center in centers:
    cluster_points = np.random.normal(loc=center, scale=std_dev, size=(points_per_cluster, 4))
    data_list.append(cluster_points)

data_array = np.vstack(data_list)
# save csv and avoid file header for easier loading in the MapReduce job
data = pd.DataFrame(data_array)
data.to_csv("kmeans_data.csv", index=False, header=False)
print("Dataset generated.")

# randomly pick 3 points as initial centroids
initial_centroids = data.sample(n=k, random_state=42).values
# preserve the same format as the data file for easier loading in the MapReduce job
initial_centroids_df = pd.DataFrame(initial_centroids, columns=data.columns)
# save initial centroids to a file
initial_centroids_df.to_csv("kmeans_seeds.csv", index=False, header=False)
print("Initial centroids saved.")
