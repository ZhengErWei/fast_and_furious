import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
import sys
from sklearn.cluster import KMeans
import numpy as np

SAMPLE_DF = pd.read_csv('sample_trip.csv')[['tpep_pickup_datetime','tpep_dropoff_datetime',
            'pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude',
                                                    'fare_amount','tip_amount']]
NONSAMPLE_DF = pd.read_csv('nonsample_trip.csv')[['tpep_pickup_datetime','tpep_dropoff_datetime',
            'pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude',
                                                    'fare_amount','tip_amount']]

def calculate_distance(lat1, lon1, lat2, lon2):
# approximate radius of earth in km
    R = 6373.0

    dlon = radians(lon2) - radians(lon1)
    dlat = radians(lat2) - radians(lat1)

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance


def k_means(df,df_pred, n):
    coordinates = df[["pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude"]]
    filter = (coordinates['pickup_latitude'] <= -66.9513812) & (coordinates['pickup_longitude'] <= 49.3457868) \
            & (coordinates['dropoff_latitude'] <= -66.9513812) & (coordinates['pickup_longitude'] <= 49.3457868)
    coordinates = coordinates[filter]
    coordinats1 = coordinates[["pickup_longitude","pickup_latitude"]]
    coordinats2 = coordinates[["dropoff_longitude","dropoff_latitude"]]
    coordinate_array1 = np.array(coordinates1)
    coordinate_array2 = np.array(coordinates2)
    kmeans1 = KMeans(n_clusters= n, random_state=0).fit(coordinate_array1)
    kmeans2 = KMeans(n_clusters= n, random_state=0).fit(coordinate_array2)
    coordinates['pickup_cluster'] = kmeans1.labels_
    coordinates['dropoff_cluster'] = kmeans2.labels_
    coordinates_pred = df_pred[["pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude"]]
    filter = (coordinates_pred['pickup_latitude'] <= -66.9513812) & (coordinates_pred['pickup_longitude'] <= 49.3457868) \
           & (coordinates_pred['dropoff_latitude'] <= -66.9513812) & (coordinates_pred['pickup_longitude'] <= 49.3457868)
    coordinates_pred = coordinates_pred[filter]
    coor_pred1 = coordinates_pred[["pickup_longitude","pickup_latitude"]]
    coor_pred2 = coordinates_pred[["dropoff_longitude","dropoff_latitude"]]
    pickup_label = kmeans1.predict(np.array(coor_pred1))
    dropoff_label = kmeans2.predict(np.array(coor_pred2))
    coordinates_pred['pickup_cluster'] = pickup_label
    coordinates_pred['dropoff_cluster'] = dropoff_label
    return coordinates, coordinates_pred

#coordinates, coordinates_pred = k_means(SAMPLE_DF, NONSAMPLE_DF)
#sample_location = coordinates[['pickup_cluster','dropoff_cluster']]
#nonsample_location = coordinates_pred[['pickup_cluster','dropoff_cluster']]


def get_sample_subset(y, df, location_df):

    if y == 'tip':
        rv = df['tip_amount'] / df['fare_amount']
    else:
        distance = df.apply(lambda x: calculate_distance(x['pickup_longitude'], x['pickup_latitude'], 
                                                                x['dropoff_longitude'], x['dropoff_latitude']), axis= 1)

        start_t = pd.to_datetime(df['tpep_pickup_datetime'], format = '%Y-%m-%d %H:%M:%S')
        end_t = pd.to_datetime(df['tpep_dropoff_datetime'], format = '%Y-%m-%d %H:%M:%S')
        time_diff =  end_t - start_t
        time_diff = time_diff.apply(lambda x: x.total_seconds()/60)
        rv = time_diff/distance
    df_new = pd.concat([rv, location_df], axis = 1)
    df_new = df.dropna(how='any')
    df_new.columns = ['val', 'pickup_cluster', 'dropoff_cluster']


    return df_new


 if __name__ == '__main__':
 	y, outputfile = sys.argv[1:]
 	coordinates, coordinates_pred = k_means(SAMPLE_DF, NONSAMPLE_DF)
    sample_location = coordinates[['pickup_cluster','dropoff_cluster']]
    nonsample_location = coordinates_pred[['pickup_cluster','dropoff_cluster']]
 	df_sample =  get_sample_subset(y, SAMPLE_DF, sample_location)
 	df_nonsample = get_sample_subset(y, NONSAMPLE_DF, nonsample_location)
 	df = pd.concat([df_sample,df_nonsample]).reset_index(drop=True)
 	df.to_csv(outputfile)
