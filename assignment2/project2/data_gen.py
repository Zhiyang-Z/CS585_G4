# PART 1

# https://numpy.org/doc/2.1/reference/random/generated/numpy.random.uniform.html

import numpy as np
import pandas as pd
np.random.seed(42) #for reproducibility

# a dataset of at least 6,000 points
pointsNum = 6000
k = 3

# for verification purpose, we select 3 centers and generate points around them
centers = np.array([[2000, 200000, 2000, 200000], [4000, 400000, 4000, 400000], [6000, 600000, 6000, 600000]])
points_per_center = pointsNum // len(centers)
# generate points around each center with Gaussian distribution
w = []
x = []
y = []
z = []
for center in centers:
    w.extend(np.random.normal(loc=center[0], scale=50, size=points_per_center))
    x.extend(np.random.normal(loc=center[1], scale=5000, size=points_per_center))
    y.extend(np.random.normal(loc=center[2], scale=50, size=points_per_center))
    z.extend(np.random.normal(loc=center[3], scale=5000, size=points_per_center))

data = pd.DataFrame({
    'w': w,
    'x': x,
    'y': y,
    'z': z,
})
# save csv and avoid file header for easier loading in the MapReduce job
data.to_csv("kmeans_data.csv", index=False, header=False)
print("Dataset generated.")

# randomly pick 3 points as initial centroids
initial_centroids = data.sample(n=k, random_state=42).values
# preserve the same format as the data file for easier loading in the MapReduce job
initial_centroids_df = pd.DataFrame(initial_centroids, columns=data.columns)
# save initial centroids to a file
initial_centroids_df.to_csv("kmeans_seeds.csv", index=False, header=False)
print("Initial centroids saved.")
