import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
import sys

WEATHER_DF = pd.read_csv('../data/sample_weather.csv')[['s_cond', 's_visi']]
SAMPLE_DF = pd.read_csv('../data/sample_trip.csv')[['tpep_pickup_datetime', \
 													'tpep_dropoff_datetime', \
 													'pickup_longitude', \
 													'pickup_latitude', \
 													'dropoff_longitude', \
 													'dropoff_latitude', \
 													'fare_amount', \
 													'tip_amount']]



def calculate_distance(lat1, lon1, lat2, lon2):
# approximate radius of earth in km
	R = 6373.0

	dlon = radians(lon2) - radians(lon1)
	dlat = radians(lat2) - radians(lat1)

	a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
	c = 2 * atan2(sqrt(a), sqrt(1 - a))

	distance = R * c

	return distance



def get_sample_subset(y):

	if y == 'tip':
		rv = SAMPLE_DF['tip_amount'] / SAMPLE_DF['fare_amount']
	else:
		distance = SAMPLE_DF.apply(lambda x: \
							   calculate_distance(x['pickup_longitude'], 
												  x['pickup_latitude'], 
												  x['dropoff_longitude'], 
												  x['dropoff_latitude']), axis= 1)

		start_t = pd.to_datetime(SAMPLE_DF['tpep_pickup_datetime'], \
						 		 format = '%Y-%m-%d %H:%M:%S')
		end_t = pd.to_datetime(SAMPLE_DF['tpep_dropoff_datetime'], \
					             format = '%Y-%m-%d %H:%M:%S')
		time_diff =  end_t - start_t
		time_diff = time_diff.apply(lambda x: x.total_seconds()/60)
		rv = time_diff/distance

	df = pd.concat([rv, WEATHER_DF], axis = 1)
	df = df.dropna(how='any')
	df.columns = ['val', 's_cond', 's_visi']
	df = df[df['val'] != float('inf')]

	return df


if __name__ == '__main__':
	y, outputfile = sys.argv[1:]
	df = get_sample_subset(y)
	df.to_csv(outputfile)

