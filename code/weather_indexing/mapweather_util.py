import pandas as pd
from datetime import datetime


WEATHER_DF = pd.read_csv('../../data/weather_201507_201606.csv')
WEATHER_DF.columns = ['date', 'hour', 'minute', 'visibility', 'cond']
WEATHER_DF['visibility'].replace(-9999, 10, inplace=True)
WEATHER_DF['date'] = WEATHER_DF['date'].astype(str)
WEATHER_DF['hour'] = WEATHER_DF['hour'].astype(str)
WEATHER_DF['minute'] = WEATHER_DF['minute'].astype(str)
for ind, row in WEATHER_DF.iterrows():
	if row['cond'] == 'Unknown':
		WEATHER_DF.loc[ind, 'cond'] = WEATHER_DF.iloc[ind - 1]['cond']

# TRIP_DF = pd.read_csv('../data/yellow_tripdata_2016-01.csv')
# TRIP_DF = TRIP_DF.dropna(how='any')
# TARGET_DF = TRIP_DF[['tpep_pickup_datetime', 'tpep_dropoff_datetime']]

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


# cond_val = array(['Overcast', 'Partly Cloudy', 'Clear', 'Scattered Clouds',
#        'Mostly Cloudy', 'Light Rain', 'Haze', 'Rain', 'Heavy Rain',
#        'Light Snow', 'Snow', 'Heavy Snow', 'Light Freezing Fog',
#        'Light Freezing Rain', 'Fog'], dtype=object)




