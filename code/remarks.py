#  Here we just use the df to represent sample dataset

import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
import sys
from sklearn.cluster import KMeans
import numpy as np


coordinates = df[["pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude"]]
filter = (coordinates['pickup_latitude'] <= -66.9513812) & (coordinates['pickup_longitude'] <= 49.3457868) \
            & (coordinates['dropoff_latitude'] <= -66.9513812) & (coordinates['pickup_longitude'] <= 49.3457868)
coordinates = coordinates[filter]
coordinats1 = coordinates[["pickup_longitude","pickup_latitude"]]
coordinats2 = coordinates[["dropoff_longitude","dropoff_latitude"]]
coordinate_array1 = np.array(coordinates1)
coordinate_array2 = np.array(coordinates2)
kmeans1 = KMeans(n_clusters= 100, random_state=0).fit(coordinate_array1)
kmeans2 = KMeans(n_clusters= 100, random_state=0).fit(coordinate_array2)



# if you need to use the kmeans model in mapreduce, just call 

label = kmeans1.predict(np.array(longtitude, latitude))
label = kmeans2.predict(np.array(longtitude, latitude))