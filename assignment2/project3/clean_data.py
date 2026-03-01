# PART 1
import pandas as pd
import numpy as np
np.random.seed(42) #for reproducibility
#number of clusters
k = 3

#dataset
data = pd.read_csv("data.csv", header=None)

# delete the first 2 columns (ID and Customer key)
data = data.drop(columns=[0,1])
data.to_csv("kmeans_data.csv", index=False, header=False)

#randomly pick 3 points as initial centroids
initial_centroids = data.sample(n=k, random_state=42)

#save initial centroids to a file
initial_centroids.to_csv("kmeans_seeds.csv", index=False, header=False)

print("kmeans dataset is generated")
