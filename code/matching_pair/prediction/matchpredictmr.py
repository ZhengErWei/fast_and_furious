# Purpose: use this file to get value of y of the most similar trip
#          after comparing each index of each row in a given file
#          and get the match with smallest differnce of each index.
#          use the y as prediction

# command: python3 thisfile inputdata
# Input: files can be found in .../../../data (sample_trip_2.csv, raw_time_sample.csv)
# Output: ../../../data/match_mr.txt


from mrjob.job import MRJob
import csv
import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
from sklearn.cluster import KMeans
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY, HOURLY


class MRmatch(MRJob):

	# same code as mapindex.py
	# all data should be put in the same folder
	def mapper_init(self):

		# all data files should be put int the same folder
		self.df = pd.read_csv('sample_trip.csv')
		self.coordinates = self.df[["pickup_longitude", 
							   		"pickup_latitude",
							   		"dropoff_longitude",
							   		"dropoff_latitude"]]
		self.coordinates1 = self.coordinates[["pickup_longitude",
											  "pickup_latitude"]]
		self.coordinates2 = self.coordinates[["dropoff_longitude",
											  "dropoff_latitude"]]
		self.coordinate_array1 = np.array(self.coordinates1)
		self.coordinate_array2 = np.array(self.coordinates2)
		self.kmeans_1 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array1)
		self.kmeans_2 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array2)

		self.weather_df = pd.read_csv('weather_201507_201606.csv')
		self.weather_df.columns = ['date', 'hour', 'minute', 'visibility', 'cond']
		self.weather_df['visibility'].replace(-9999, 10, inplace=True)
		self.weather_df['date'] = self.weather_df['date'].astype(str)
		self.weather_df['hour'] = self.weather_df['hour'].astype(str)
		self.weather_df['minute'] = self.weather_df['minute'].astype(str)
		for ind, row in self.WEATHER_DF.iterrows():
			if row['cond'] == 'Unknown':
				self.weather_df.loc[ind, 'cond'] = \
				self.weather_df.iloc[ind - 1]['cond']


		self.hour_list = []
		self.start = date(2015, 7, 1)
		self.end = date(2016, 6, 30)
		for dt in rrule(HOURLY, dtstart = self.start, until = self.end):
			self.hour_list.append(dt.strftime("%Y-%m-%d-%H"))

		self.weather_dict = {}
		for ind, row in self.weather_df.iterrows():
			key = (row['date'], row['hour'])
			value = (row['cond'], row['visibility'])
			self.weather_dict[key] = self.weather_dict.get(key, value)

		self.time_weather_dict = {}
		for time in self.hour_list:
			time_split = time.split('-')
			day = ''.join(time_split[:3])
			hour = str(int(time_split[-1]))
			key = (day, hour)
			if key not in self.weather_dict.keys():
				new_key = self.get_last_hour(key)
				while new_key not in self.weather_dict.keys():
					new_key = self.get_last_hour(new_key)
				self.time_weather_dict[key] = self.weather_dict[new_key]
			else:
				self.time_weather_dict[key] = self.weather_dict[key]


		self.weather_index = pd.read_csv('weather_index_time.csv', header=None)
		self.location_index = pd.read_csv('location_index_time.csv', header=None)
		self.hour_index = pd.read_csv('hour_index_time.csv', header=None)
		self.month_index = pd.read_csv('month_index_time.csv', header=None)
		self.week_index = pd.read_csv('weekday_index_time.csv', header=None)

		self.weather_index.columns = ['cond', 'visi', 'index']
		self.weather_index_dict = {}
		for ind, row in self.weather_index.iterrows():
			key = (row['cond'], row['visi'])
			value = row['index']
			self.weather_index_dict[key] = value

		self.location_index.columns = ['pick', 'drop', 'index']
		self.location_index_dict = {}
		for ind, row in self.location_index.iterrows():
			key = (row['pick'], row['drop'])
			value = row['index']
			self.location_index_dict[key] = value

		self.hour_index.columns = ['hour', 'index']
		self.hour_index.set_index('hour')
		self.month_index.columns = ['month', 'index']
		self.month_index.set_index('month')
		self.week_index.columns = ['day', 'index']
		self.week_index.set_index('day')

		# hard code here to choose the file you want to get the matching pair
		self.filenames = ['raw_time_total_sample.csv', 'raw_time_total_sample_2.csv']

		self.min_diff = [6] * 6


	# same code as mapindex.py
	def get_last_hour(self, key):
		'''
		Get the hour before the given hour

		Input:
			key: ('20170101', '1')

		Return: tuple
		'''

		s = key[0] + '-' + key[1]
		time_obj = datetime.strptime(s, '%Y%m%d-%H')
		last_hour = time_obj - timedelta(hours=1)
		new_s = last_hour.strftime('%Y%m%d-%H')
		new_s_split = new_s.split('-')
		new_key = (new_s_split[0], str(int(new_s_split[1])))

		return new_key


	# this function copied from stackflow
	def calculate_distance(self, lat1, lon1, lat2, lon2):
		'''
		Calculate the distance in mile between the given
		longitude latitude between the start and end points
		'''

		R = 6373.0

		dlon = radians(lon2) - radians(lon1)
		dlat = radians(lat2) - radians(lat1)

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c

		return distance


	# same code as mapindex_util
	def get_index(self, row):

		# when deal with original taxi file all index after row plus one
		start_date, start_hour = row[2].split(':')[0].split(' ')
		start_hour = str(int(start_hour))
		year, month, date = start_date.split('-')
		start_date = ''.join([year, month, date])
		end_date, end_hour = row[3].split(':')[0].split(' ')
		end_date = ''.join(end_date.split('-'))
		end_hour = str(int(end_hour))

		pick_lon = float(row[6])
		pick_lat = float(row[7])
		drop_lon = float(row[10])
		drop_lat = float(row[11])

		#weather index
		start_weather_tup = (start_date, start_hour)
		start_cond = self.time_weather_dict[start_weather_tup]
		weather_st_ind = self.weather_index_dict[start_cond]

		end_weather_tup = (end_date, end_hour)
		end_cond = self.time_weather_dict[end_weather_tup]
		weather_end_ind = self.weather_index_dict[end_cond]

		#location index
		pick_clus = self.kmeans_1.predict(np.array([pick_lon, \
													pick_lat]).reshape(1,-1))
		drop_clus = self.kmeans_2.predict(np.array([drop_lon, \
													drop_lat]).reshape(1,-1))
		loc_ind = self.location_index_dict[(pick_clus[0], drop_clus[0])]

		#time index
		weekday = datetime.strptime(start_date, '%Y%m%d').weekday()
		weekday_ind = self.week_index.loc[weekday]['index']

		hour_ind = self.hour_index.loc[int(start_hour)]['index']

		month_ind = self.month_index.loc[int(month)]['index']

		key = (weather_st_ind, weather_end_ind, loc_ind, weekday_ind, \
			   hour_ind, month_ind)

		# when y is traffic time
		distance = self.calculate_distance(pick_lat, pick_lon, \
										   drop_lat, drop_lat)
		start_time = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
		end_time = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
		time_diff = (end_time - start_time).total_seconds()/60
		value = time_diff/distance

		# when y is tip rate
		#value = float(row[16])/float(row[13])

		return key, value



	def clean_raw_ind_data(self, row):
		'''
		clean the output from mapreduce.
		'''

		# ind_1 = row[0].strip("'[")
		# ind_2 = row[1].strip(" '")
		# ind_3 = row[2].strip(" '")
		# ind_4 = row[3].strip(" '")
		# ind_5 = row[4].strip(" '")
		# ind_6, y = row[5].strip(" '").split(']\t')

		ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y = row[1:]

		rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
		rv = [float(a) for a in rv]

		return rv


	def get_diff(self, row1, row2):
		'''
		Get the difference of all index of two rows
		'''

		rv = [abs(i - j) for i, j in zip(row1, row2)]

		return rv

	def abs_smaller(self, list1, list2):
		'''
		To decide whether each entry of list2 
		is smaller that that of list1
		'''

		for i in range(6):
			if list1[i] > list2[i]:
				return False

		return True

	def is_smallest(self, list1, list2, diff_standard):
		'''
		To decide whether each entrty of list2 is smaller
		than list1, if yes, update the current minimum 
		difference.
		'''

		diff = self.get_diff(list1, list2)
		if self.abs_smaller(diff, diff_standard):
			return diff
		else:
			False





	def mapper(self, _, line):

		row = next(csv.reader([line]))
		if (len(row) > 0) and (row[1] != 'VendorID'):
			try:
				key, value = self.get_index(row)
				key_list = [a for a in key] + [value]
				for filename in self.filenames:
					dir_name = filename
					with open(dir_name, 'r') as f:
						rb = csv.reader(f)
						for row_ind in rb:
							ind_list = self.clean_raw_ind_data(row_ind)
							result = self.is_smallest(key, ind_list[:6], self.min_diff)
							if result:
								self.min_diff = result
								rv = ind_list

				yield row, ind_list
									
			except:
				row = None

	# same as mapper_init			
	def reducer_init(self):

		self.df = pd.read_csv('sample_trip.csv')
		self.coordinates = self.df[["pickup_longitude", 
							   		"pickup_latitude",
							   		"dropoff_longitude",
							   		"dropoff_latitude"]]
		self.coordinates1 = self.coordinates[["pickup_longitude",
											  "pickup_latitude"]]
		self.coordinates2 = self.coordinates[["dropoff_longitude",
											  "dropoff_latitude"]]
		self.coordinate_array1 = np.array(self.coordinates1)
		self.coordinate_array2 = np.array(self.coordinates2)
		self.kmeans_1 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array1)
		self.kmeans_2 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array2)

		self.weather_df = pd.read_csv('weather_201507_201606.csv')
		self.weather_df.columns = ['date', 'hour', 'minute', 'visibility', 'cond']
		self.weather_df['visibility'].replace(-9999, 10, inplace=True)
		self.weather_df['date'] = self.weather_df['date'].astype(str)
		self.weather_df['hour'] = self.weather_df['hour'].astype(str)
		self.weather_df['minute'] = self.weather_df['minute'].astype(str)
		for ind, row in self.WEATHER_DF.iterrows():
			if row['cond'] == 'Unknown':
				self.weather_df.loc[ind, 'cond'] = \
				self.weather_df.iloc[ind - 1]['cond']


		self.hour_list = []
		self.start = date(2015, 7, 1)
		self.end = date(2016, 6, 30)
		for dt in rrule(HOURLY, dtstart = self.start, until = self.end):
			self.hour_list.append(dt.strftime("%Y-%m-%d-%H"))

		self.weather_dict = {}
		for ind, row in self.weather_df.iterrows():
			key = (row['date'], row['hour'])
			value = (row['cond'], row['visibility'])
			self.weather_dict[key] = self.weather_dict.get(key, value)

		self.time_weather_dict = {}
		for time in self.hour_list:
			time_split = time.split('-')
			day = ''.join(time_split[:3])
			hour = str(int(time_split[-1]))
			key = (day, hour)
			if key not in self.weather_dict.keys():
				new_key = self.get_last_hour(key)
				while new_key not in self.weather_dict.keys():
					new_key = self.get_last_hour(new_key)
				self.time_weather_dict[key] = self.weather_dict[new_key]
			else:
				self.time_weather_dict[key] = self.weather_dict[key]


		self.weather_index = pd.read_csv('weather_index_time.csv', header=None)
		self.location_index = pd.read_csv('location_index_time.csv', header=None)
		self.hour_index = pd.read_csv('hour_index_time.csv', header=None)
		self.month_index = pd.read_csv('month_index_time.csv', header=None)
		self.week_index = pd.read_csv('weekday_index_time.csv', header=None)

		self.weather_index.columns = ['cond', 'visi', 'index']
		self.weather_index_dict = {}
		for ind, row in self.weather_index.iterrows():
			key = (row['cond'], row['visi'])
			value = row['index']
			self.weather_index_dict[key] = value

		self.location_index.columns = ['pick', 'drop', 'index']
		self.location_index_dict = {}
		for ind, row in self.location_index.iterrows():
			key = (row['pick'], row['drop'])
			value = row['index']
			self.location_index_dict[key] = value

		self.hour_index.columns = ['hour', 'index']
		self.hour_index.set_index('hour')
		self.month_index.columns = ['month', 'index']
		self.month_index.set_index('month')
		self.week_index.columns = ['day', 'index']
		self.week_index.set_index('day')

		# hard code here to choose the file you want to get the matching pair
		self.filenames = ['raw_time_total_sample.csv', 'raw_time_total_sample_2.csv']

		self.min_diff = [6] * 6


# yield each trip and pred dependent y
# output format see ../data/matchmr_sample.csv

	def reducer(self, row, lists):

		key, value = self.get_index(row)
		for ind_list in lists:
			result = self.is_smallest(key, ind_list[:6], self.min_diff)
			if result:
				rv = ind_list[6]

		yield row, rv



if __name__ == '__main__':
	MRmatch.run()
