import pandas as pd 
import sys
sys.path.append('weather_indexing')
import mapweather_util
sys.path.append('location_indexing')
import csv
import pandas as pd
import remarks
import numpy as np
from datetime import datetime
import sample_y


TIME_WEATHER_DICT = mapweather_util.get_weather_hour_dict()

WEATHER_INDEX = pd.read_csv('../data/weather_index/weather_index_time.csv', header=None)
LOCATION_INDEX = pd.read_csv('../data/location_index/location_index_time.csv', header=None)
HOUR_INDEX = pd.read_csv('../data/time_index/hour_index_time.csv', header=None)
MONTH_INDEX = pd.read_csv('../data/time_index/month_index_time.csv', header=None)
WEEK_INDEX = pd.read_csv('../data/time_index/weekday_index_time.csv', header=None)

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


KMEANS_1 = remarks.kmeans1
KMEANS_2 = remarks.kmeans2



def get_index(row):

	start_date, start_hour = row[1].split(':')[0].split(' ')
	start_hour = str(int(start_hour))
	year, month, date = start_date.split('-')
	start_date = ''.join([year, month, date])
	end_date, end_hour = row[2].split(':')[0].split(' ')
	end_date = ''.join(end_date.split('-'))
	end_hour = str(int(end_hour))

	pick_lon = float(row[5])
	pick_lat = float(row[6])
	drop_lon = float(row[9])
	drop_lat = float(row[10])

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

	distance = sample_y.calculate_distance(pick_lat, pick_lon, drop_lat, drop_lat)
	start_time = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
	end_time = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
	time_diff = (end_time - start_time).total_seconds()/60
	value = time_diff/distance

	return key, value






