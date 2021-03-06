
# coding: utf-8

# In[5]:


import pandas as pd
import datetime
from math import sin, cos, sqrt, atan2, radians


def calculate_distance(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    dlon = radians(lon2) - radians(lon1)
    dlat = radians(lat2) - radians(lat1)

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return R * c


def calculate_time(SAMPLE_DF):
    SAMPLE_DF['start_t'] = pd.to_datetime(SAMPLE_DF['tpep_pickup_datetime'], format = '%Y-%m-%d %H:%M:%S')
    SAMPLE_DF['end_t'] = pd.to_datetime(SAMPLE_DF['tpep_dropoff_datetime'], format = '%Y-%m-%d %H:%M:%S')
    SAMPLE_DF['time_diff'] = SAMPLE_DF['end_t'] - SAMPLE_DF['start_t']
    SAMPLE_DF['time_diff'] = SAMPLE_DF['time_diff'].apply(lambda x: x.total_seconds()/60)


def add_hour(row):
    hour = int(row['tpep_pickup_datetime'].split()[1].split(':')[0])
    return hour    

def add_weekday(row):
    temp = [int(d) for d in row['tpep_pickup_datetime'].split()[0].split('-')]
    weekday = datetime.date(temp[0], temp[1], temp[2]).isoweekday()
    return weekday    

def add_month(row):
    month = int(row['tpep_pickup_datetime'].split()[0].split('-')[1])
    return month


def map_new_item(SAMPLE_DF):
    SAMPLE_DF['distance'] = SAMPLE_DF.apply(lambda x: calculate_distance(x['pickup_longitude'], x['pickup_latitude'], x['dropoff_longitude'], x['dropoff_latitude']), axis= 1)
    calculate_time(SAMPLE_DF)
    SAMPLE_DF['unit_time'] = SAMPLE_DF['time_diff']/SAMPLE_DF['distance']
    SAMPLE_DF['unit_tip'] = SAMPLE_DF['tip_amount']/SAMPLE_DF['fare_amount']
    SAMPLE_DF['hour'] = SAMPLE_DF.apply(lambda x: add_hour(x), axis= 1)
    SAMPLE_DF['weekday'] = SAMPLE_DF.apply(lambda x: add_weekday(x), axis= 1)
    SAMPLE_DF['month'] = SAMPLE_DF.apply(lambda x: add_month(x), axis= 1)
    SAMPLE_DF = SAMPLE_DF[(SAMPLE_DF['unit_time'] <= 100)]
    return SAMPLE_DF

    
def normalize(column_name, standard_name):
    df = SAMPLE_DF[standard_name].groupby(SAMPLE_DF[column_name]).mean()
    normalized_df=(df-df.min())/(df.max()-df.min())
    return normalized_df


SAMPLE_DF = pd.read_csv('sample_trip.csv')
SAMPLE_DF = map_new_item(SAMPLE_DF)
hour_index_time = normalize('hour', 'unit_time')
hour_index_tip = normalize('hour', 'unit_tip')
weekday_index_time = normalize('weekday', 'unit_time')
weekday_index_tip = normalize('weekday', 'unit_tip')
month_index_time = normalize('month', 'unit_time')
month_index_tip = normalize('month', 'unit_tip')

hour_index_time.to_csv('hour_index_time.csv')
hour_index_tip.to_csv('hour_index_tip.csv')
weekday_index_time.to_csv('weekday_index_time.csv')
weekday_index_tip.to_csv('weekday_index_tip.csv')
month_index_time.to_csv('month_index_time.csv')
month_index_tip.to_csv('month_index_tip.csv')

