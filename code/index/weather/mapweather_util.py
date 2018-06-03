# helper function used by mapweather.py and etc. 
# need to put weather_201507_2016_06.csv in ../../../data/ in the same
# folder to run the file

import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY, HOURLY


WEATHER_DF = pd.read_csv('weather_201507_201606.csv')
WEATHER_DF.columns = ['date', 'hour', 'minute', 'visibility', 'cond']
WEATHER_DF['visibility'].replace(-9999, 10, inplace=True)
WEATHER_DF['date'] = WEATHER_DF['date'].astype(str)
WEATHER_DF['hour'] = WEATHER_DF['hour'].astype(str)
WEATHER_DF['minute'] = WEATHER_DF['minute'].astype(str)
for ind, row in WEATHER_DF.iterrows():
	if row['cond'] == 'Unknown':
		WEATHER_DF.loc[ind, 'cond'] = WEATHER_DF.iloc[ind - 1]['cond']


HOUR_LIST = []
START = date(2015, 7, 1)
END = date(2016, 6, 30)
for dt in rrule(HOURLY, dtstart = START, until = END):
	HOUR_LIST.append(dt.strftime("%Y-%m-%d-%H"))


WEATHER_DICT = {}
for ind, row in WEATHER_DF.iterrows():
	key = (row['date'], row['hour'])
	value = (row['cond'], row['visibility'])
	WEATHER_DICT[key] = WEATHER_DICT.get(key, value)

# for the sample data
def get_weather_df(st, et):
	'''
	Inputs:
	  st, et: string, '2016-01-01'
	 '''

	st = ''.join(st.split('-'))
	et = ''.join(et.split('-'))
	for ind, row in WEATHER_DF.iterrows():
		if row['date'] == st:
			st_id = ind
			break
	for ind, row in WEATHER_DF.iloc[st_id:].iterrows():
		if row['date'] == et:
			et_start = ind
			break
	for ind, row in WEATHER_DF.iloc[et_start:].iterrows():
		if row['date'] != et:
			et_id = ind
			break
		et_id = ind
	rv = WEATHER_DF.iloc[st_id:et_id]

	return rv


def get_weather(date_time, df):
	'''
	datetime: str, e.g. '2016-01-01 00:00:00'
	
	Return: list 
	'''

	# date_str = ''.join(date_time[:10].split('-'))
	print(date_time)
	dt_object = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
	dt_date = dt_object.date()
	size = df.shape[0]
	weather_time = pd.to_datetime(df['date'] + ' ' + df['hour'] + ':' + \
								  df['minute'])
	
	for i in range(size):
		time = weather_time.iloc[i]
		if dt_date == time.date():
			min_d = abs(time - dt_object)
			start_id = i
			break

	rv_id = start_id
	for i in range(start_id, size): 
		time = weather_time.iloc[i]
		delta = abs(time - dt_object)
		if delta <= min_d:
			min_d = delta
			rv_id = i
		else:
			break

	rv = list(df[['visibility', 'cond']].iloc[rv_id])

	return rv

def get_all_weather(target_df, weather_df):

	all_rv = []
	for ind, row in target_df.iterrows():
		start_weather = get_weather(row['tpep_pickup_datetime'], weather_df)
		end_weather = get_weather(row['tpep_dropoff_datetime'], weather_df)
		rv = start_weather + end_weather
		all_rv.append(rv)
	df = pd.DataFrame(all_rv, columns = ['s_visi', 's_cond', 'e_visi', 'e_cond'])

	return df


#for the whole dataset
def get_weather_hour_dict():

	rv = {}
	for time in HOUR_LIST:
		time_split = time.split('-')
		day = ''.join(time_split[:3])
		hour = str(int(time_split[-1]))
		key = (day, hour)
		if key not in WEATHER_DICT.keys():
			new_key = get_last_hour(key)
			while new_key not in WEATHER_DICT.keys():
				new_key = get_last_hour(new_key)
			rv[key] = WEATHER_DICT[new_key]

		else:
			rv[key] = WEATHER_DICT[key]

	return rv


def get_last_hour(key):
	'''
	input: ('20170101', '1')
	'''

	s = key[0] + '-' + key[1]
	time_obj = datetime.strptime(s, '%Y%m%d-%H')
	last_hour = time_obj - timedelta(hours=1)
	new_s = last_hour.strftime('%Y%m%d-%H')
	new_s_split = new_s.split('-')
	new_key = (new_s_split[0], str(int(new_s_split[1])))

	return new_key



# columns
# cond_val = array(['Overcast', 'Partly Cloudy', 'Clear', 'Scattered Clouds',
#        'Mostly Cloudy', 'Light Rain', 'Haze', 'Rain', 'Heavy Rain',
#        'Light Snow', 'Snow', 'Heavy Snow', 'Light Freezing Fog',
#        'Light Freezing Rain', 'Fog'], dtype=object)





