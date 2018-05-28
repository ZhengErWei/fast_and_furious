import pandas as pd 
import mapweather_util



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