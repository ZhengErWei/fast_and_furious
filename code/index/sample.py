import pandas as pd 
# # import map_weather_trip
# import numpy as np

DIR = '../../data/'



def draw_one_sample(filename):

	dirname = DIR + filename
	print(dirname)
	df = pd.read_csv(dirname)
	df = df.dropna(how='any')
	sample_df = df.sample(n = 2000000, random_state=34)

	return sample_df

# FILENAMES = ['yellow_tripdata_2015-07.csv', 'yellow_tripdata_2015-08.csv', \
# 			 'yellow_tripdata_2015-09.csv', 'yellow_tripdata_2015-10.csv', \
# 			 'yellow_tripdata_2015-11.csv', 'yellow_tripdata_2015-12.csv', \
# 			 'yellow_tripdata_2016-01.csv', 'yellow_tripdata_2016-02.csv', \
# 			 'yellow_tripdata_2016-03.csv', 'yellow_tripdata_2016-04.csv', \
# 			 'yellow_tripdata_2016-05.csv', 'yellow_tripdata_2016-06.csv']

FILENAMES = ['raw_2015_07_time.csv', 'raw_2015_08_time.csv', 'raw_2015_09_time.csv', \
			'raw_2015_10_time.csv', 'raw_2015_11_time.csv']


if __name__ == '__main__':
	df_list = []
	for file in FILENAMES:
		print(file)
		df_list.append(draw_one_sample(file))
	rv = pd.concat(df_list)
	rv.to_csv('../../data/raw_time_total_2.csv')







