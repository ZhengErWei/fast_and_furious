import pandas as pd 
# # import map_weather_trip
# import numpy as np

DIR = '../data/taxi_data/'

COLUMNS = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       	   'passenger_count', 'trip_distance', 'pickup_longitude',
           'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag',
           'dropoff_longitude', 'dropoff_latitude', 'payment_type', 
           'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
           'improvement_surcharge', 'total_amount']

# TRIP_DF = pd.read_csv('../data/yellow_tripdata_2016-01.csv')
# TRIP_DF = TRIP_DF.dropna(how='any')
# SAMPLE_DF = TRIP_DF.sample(n=1000)

# SAMPLE_DF['start_t'] = pd.to_datetime(SAMPLE_DF['tpep_pickup_datetime'], \
# 									  format = '%Y-%m-%d %H:%M:%S')
# SAMPLE_DF['end_t'] = pd.to_datetime(SAMPLE_DF['tpep_dropoff_datetime'], \
# 									format = '%Y-%m-%d %H:%M:%S')
# SAMPLE_DF['time_diff'] =  SAMPLE_DF['end_t'] - SAMPLE_DF['start_t']
# SAMPLE_DF['time_diff'] = SAMPLE_DF['time_diff'].apply(lambda x: \
# 													  x.total_seconds()/3600)
# SAMPLE_DF['speed'] = SAMPLE_DF['trip_distance']/SAMPLE_DF['time_diff']
# SAMPLE_DF.reset_index(inplace=True)
# SAMPLE_WEATHER = map_weather_trip.get_weather_df(SAMPLE_DF)

# SAMPLE_DF = pd.concat([SAMPLE_DF, SAMPLE_WEATHER], axis = 1)
# SAMPLE_DF = SAMPLE_DF[['s_cond', 'e_cond', 's_visi', 'e_visi', 'speed']]
# SAMPLE_DF['s_visi'] = np.ceil(SAMPLE_DF['s_visi'])
# SAMPLE_DF['e_visi'] = np.ceil(SAMPLE_DF['e_visi'])

# SAMPLE_DF = SAMPLE_DF.dropna(how='any')
# SAMPLE_DF.to_csv('sample_2016_1.csv', index = False, 
# 				header=['s_cond', 'e_cond', 's_visi', 'e_visi', 'speed'])


def draw_one_sample(filename):

	dirname = DIR + filename
	print(dirname)
	df = pd.read_csv(dirname)
	df = df.dropna(how='any')
	sample_df = df.sample(n = 1000)

	return sample_df

FILENAMES = ['yellow_tripdata_2015-07.csv', 'yellow_tripdata_2015-08.csv', \
			 'yellow_tripdata_2015-09.csv', 'yellow_tripdata_2015-10.csv', \
			 'yellow_tripdata_2015-11.csv', 'yellow_tripdata_2015-12.csv', \
			 'yellow_tripdata_2016-01.csv', 'yellow_tripdata_2016-02.csv', \
			 'yellow_tripdata_2016-03.csv', 'yellow_tripdata_2016-04.csv', \
			 'yellow_tripdata_2016-05.csv', 'yellow_tripdata_2016-06.csv']


if __name__ == '__main__':
	df_list = []
	for file in FILENAMES:
		df_list.append(draw_one_sample(file))
	rv = pd.concat(df_list)
	rv.to_csv('../data/sample_trip.csv')







