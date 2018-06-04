# Purpose: get the value of dependent variable (tip rate or traffic time) 
#          with the location(pick-up and drop-off) clusters of the taxi data
#          record in a given data file.

import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
import sys
from sklearn.cluster import KMeans
import numpy as np


# Define sample dataframe, which will be used in this part to train kmeans model to get the
# pick-up and drop-off location clusters
SAMPLE_DF = pd.read_csv('../../data/sample_trip.csv')[['tpep_pickup_datetime','tpep_dropoff_datetime',
            'pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude',
                                                    'fare_amount','tip_amount']]



def calculate_distance(lat1, lon1, lat2, lon2):
    '''
    The function is used to calculate the distance between two locations given
    their longitude and latitude.
    
    Input:
    lat1: The latitude of the first point
    lon1: The longitude of the first point
    lat2: The latitude of the second point
    lon2: The longitude of the second point
    
    Return:
    distance: The distance between the input two points
    '''
# approximate radius of earth in km
    R = 6373.0

    dlon = radians(lon2) - radians(lon1)
    dlat = radians(lat2) - radians(lat1)

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance


def k_means(df, n):
    '''
    The function is used to classify and get the n clusters of the pick-up location and drop-off
    location of trip records in the input dataframe using kmeans model
    
    Inputs:
    df: The dataframe used to train the kmeans model
    n: The number of the clusters in the kmeans model
    
    Return:
    coordinates: A dataframe contains the longitude and latitude of the pick-up location 
                 and drop-off location, along with their locations clusters trained by kmeans
                 model 
    '''
    coordinates = df[["pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude"]]

    # filter = (coordinates['pickup_latitude'] <= -66.9513812) & (coordinates['pickup_longitude'] <= 49.3457868) \
    #         &(coordinates['dropoff_latitude'] <= -66.9513812) & (coordinates['pickup_longitude'] <= 49.3457868)
    # coordinates = coordinates[filter]

    coordinates1 = coordinates[["pickup_longitude","pickup_latitude"]]

    coordinates2 = coordinates[["dropoff_longitude","dropoff_latitude"]]
    coordinate_array1 = np.array(coordinates1)
    coordinate_array2 = np.array(coordinates2)
    kmeans1 = KMeans(n_clusters= n, random_state=0).fit(coordinate_array1)
    kmeans2 = KMeans(n_clusters= n, random_state=0).fit(coordinate_array2)
    coordinates['pickup_cluster'] = kmeans1.labels_
    coordinates['dropoff_cluster'] = kmeans2.labels_
    return coordinates


def get_sample_subset(y, df, location_df):
    '''
    The function is used to get the dataframe of all dependent variable values with the 
    location condition (e.g. the pick-up cluster and the drop-off cluster)
    
    Input:
    y: the string, 'tip' or 'time'
    df: the dataframe used to calculate the dependent variable
    location_df: the dataframe contains the location condition 
    
    Retrun: 
    df_new: a new dataframe incluing the dependent variable and its related location cluster
    '''

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
    df_new = df_new.dropna(how='any')
    df_new.columns = ['val', 'pickup_cluster', 'dropoff_cluster']


    return df_new


if __name__ == '__main__':
    y, outputfile = sys.argv[1:]
    coordinates = k_means(SAMPLE_DF, 50)
    sample_location = coordinates[['pickup_cluster','dropoff_cluster']]
    print(sample_location.shape)
    df_sample =  get_sample_subset(y, SAMPLE_DF, sample_location)
    df_sample.to_csv(outputfile)
