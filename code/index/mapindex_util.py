# include funcitons and datastructre to be included by matchpredictmpi.py
# and others, it should be put in the same folder when imported
import pandas as pd 
import sys
import mapweather_util
import csv
import pandas as pd
import numpy as np
from datetime import datetime
import sample_y
from math import sin, cos, sqrt, atan2, radians
from sklearn.cluster import KMeans
import numpy as np


TIME_WEATHER_DICT = mapweather_util.get_weather_hour_dict()

# the files can be found in ../data
WEATHER_INDEX = pd.read_csv('weather_index_time.csv', header=None)
LOCATION_INDEX = pd.read_csv('location_index_time.csv', header=None)
HOUR_INDEX = pd.read_csv('hour_index_time.csv', header=None)
MONTH_INDEX = pd.read_csv('month_index_time.csv', header=None)
WEEK_INDEX = pd.read_csv('weekday_index_time.csv', header=None)

WEATHER_INDEX.columns = ['cond', 'visi', 'index']
WEATHER_INDEX_DICT = {}
for ind, row in WEATHER_INDEX.iterrows():
	key = (row['cond'], row['visi'])
	value = row['index']
	WEATHER_INDEX_DICT[key] = value

LOCATION_INDEX.columns = ['pick', 'drop', 'index']
LOCATION_INDEX_DICT = {}
for ind, row in LOCATION_INDEX.iterrows():
	key = (row['pick'], row['drop'])
	value = row['index']
	LOCATION_INDEX_DICT[key] = value

HOUR_INDEX.columns = ['hour', 'index']
HOUR_INDEX.set_index('hour')
MONTH_INDEX.columns = ['month', 'index']
MONTH_INDEX.set_index('month')
WEEK_INDEX.columns = ['day', 'index']
WEEK_INDEX.set_index('day')



# k means to get cluster label
CLUSTER_DF = pd.read_csv('sample_trip.csv')
COOR = CLUSTER_DF [["pickup_longitude","pickup_latitude","dropoff_longitude",
				  	 	   "dropoff_latitude"]]
COOR_1 = COOR[["pickup_longitude","pickup_latitude"]]
COOR_2 = COOR[["dropoff_longitude","dropoff_latitude"]]
COOR_ARRAY_1 = np.array(COOR_1)
COOR_ARRAY_2 = np.array(COOR_2)
KMEANS_1 = KMeans(n_clusters= 50, random_state=0).fit(COOR_ARRAY_1)
KMEANS_2 = KMeans(n_clusters= 50, random_state=0).fit(COOR_ARRAY_2)



def get_index(row):

	start_date, start_hour = row[2].split(':')[0].split(' ')
	start_hour = str(int(start_hour))
	year, month, date = start_date.split('-')
	start_date = ''.join([year, month, date])
	end_date, end_hour = row[3].split(':')[0].split(' ')
	end_date = ''.join(end_date.split('-'))
	end_hour = str(int(end_hour))

    # if work on larger dataset all index of row[] minus one
	pick_lon = float(row[6])
	pick_lat = float(row[7])
	drop_lon = float(row[10])
	drop_lat = float(row[11])

	#weather index
	start_weather_tup = (start_date, start_hour)
	start_cond = TIME_WEATHER_DICT[start_weather_tup]
	weather_st_ind = WEATHER_INDEX_DICT[start_cond]

	end_weather_tup = (end_date, end_hour)
	end_cond = TIME_WEATHER_DICT[end_weather_tup]
	weather_end_ind = WEATHER_INDEX_DICT[end_cond]

	#location index
	pick_clus = KMEANS_1.predict(np.array([pick_lon, pick_lat]).reshape(1,-1))
	drop_clus = KMEANS_2.predict(np.array([drop_lon, drop_lat]).reshape(1,-1))
	loc_ind = LOCATION_INDEX_DICT[(pick_clus[0], drop_clus[0])]

	#time index
	weekday = datetime.strptime(start_date, '%Y%m%d').weekday()
	weekday_ind = WEEK_INDEX.loc[weekday]['index']

	hour_ind = HOUR_INDEX.loc[int(start_hour)]['index']

	month_ind = MONTH_INDEX.loc[int(month)]['index']

	key = (weather_st_ind, weather_end_ind, loc_ind, weekday_ind, hour_ind, month_ind)

	# for travel time
	distance = sample_y.calculate_distance(pick_lat, pick_lon, drop_lat, drop_lat)
	start_time = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
	end_time = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
	time_diff = (end_time - start_time).total_seconds()/60
	value = time_diff/distance

	# for tip rate
	# value = row[16]/row[13]

	return key, value








