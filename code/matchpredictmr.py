from mrjob.job import MRJob
import csv
import pandas as pd 
from math import sin, cos, sqrt, atan2, radians
from sklearn.cluster import KMeans
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY, HOURLY


class MRmatch(MRJob):


	def mapper_init(self):

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
		self.KMEANS_1 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array1)
		self.KMEANS_2 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array2)

		self.WEATHER_DF = pd.read_csv('weather_201507_201606.csv')
		self.WEATHER_DF.columns = ['date', 'hour', 'minute', 'visibility', 'cond']
		self.WEATHER_DF['visibility'].replace(-9999, 10, inplace=True)
		self.WEATHER_DF['date'] = self.WEATHER_DF['date'].astype(str)
		self.WEATHER_DF['hour'] = self.WEATHER_DF['hour'].astype(str)
		self.WEATHER_DF['minute'] = self.WEATHER_DF['minute'].astype(str)
		for ind, row in self.WEATHER_DF.iterrows():
			if row['cond'] == 'Unknown':
				self.WEATHER_DF.loc[ind, 'cond'] = self.WEATHER_DF.iloc[ind - 1]['cond']


		self.HOUR_LIST = []
		self.START = date(2015, 7, 1)
		self.END = date(2016, 6, 30)
		for dt in rrule(HOURLY, dtstart = self.START, until = self.END):
			self.HOUR_LIST.append(dt.strftime("%Y-%m-%d-%H"))

		self.WEATHER_DICT = {}
		for ind, row in self.WEATHER_DF.iterrows():
			key = (row['date'], row['hour'])
			value = (row['cond'], row['visibility'])
			self.WEATHER_DICT[key] = self.WEATHER_DICT.get(key, value)

		self.TIME_WEATHER_DICT = {}
		for time in self.HOUR_LIST:
			time_split = time.split('-')
			day = ''.join(time_split[:3])
			hour = str(int(time_split[-1]))
			key = (day, hour)
			if key not in self.WEATHER_DICT.keys():
				new_key = self.get_last_hour(key)
				while new_key not in self.WEATHER_DICT.keys():
					new_key = self.get_last_hour(new_key)
				self.TIME_WEATHER_DICT[key] = self.WEATHER_DICT[new_key]
			else:
				self.TIME_WEATHER_DICT[key] = self.WEATHER_DICT[key]


		self.WEATHER_INDEX = pd.read_csv('weather_index_time.csv', header=None)
		self.LOCATION_INDEX = pd.read_csv('location_index_time.csv', header=None)
		self.HOUR_INDEX = pd.read_csv('hour_index_time.csv', header=None)
		self.MONTH_INDEX = pd.read_csv('month_index_time.csv', header=None)
		self.WEEK_INDEX = pd.read_csv('weekday_index_time.csv', header=None)

		self.WEATHER_INDEX.columns = ['cond', 'visi', 'index']
		self.WEATHER_INDEX_DICT = {}
		for ind, row in self.WEATHER_INDEX.iterrows():
			key = (row['cond'], row['visi'])
			value = row['index']
			self.WEATHER_INDEX_DICT[key] = value

		self.LOCATION_INDEX.columns = ['pick', 'drop', 'index']
		self.LOCATION_INDEX_DICT = {}
		for ind, row in self.LOCATION_INDEX.iterrows():
			key = (row['pick'], row['drop'])
			value = row['index']
			self.LOCATION_INDEX_DICT[key] = value

		self.HOUR_INDEX.columns = ['hour', 'index']
		self.HOUR_INDEX.set_index('hour')
		self.MONTH_INDEX.columns = ['month', 'index']
		self.MONTH_INDEX.set_index('month')
		self.WEEK_INDEX.columns = ['day', 'index']
		self.WEEK_INDEX.set_index('day')

		# self.filenames = ['raw_2015_08_time.csv']
		self.filenames = ['raw_sample_time.csv']

		self.min_diff = [6] * 6



	def get_last_hour(self, key):
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


	def calculate_distance(self, lat1, lon1, lat2, lon2):
	# approximate radius of earth in km
		R = 6373.0

		dlon = radians(lon2) - radians(lon1)
		dlat = radians(lat2) - radians(lat1)

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c

		return distance


	def get_index(self, row):


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
		start_cond = self.TIME_WEATHER_DICT[start_weather_tup]
		weather_st_ind = self.WEATHER_INDEX_DICT[start_cond]

		end_weather_tup = (end_date, end_hour)
		end_cond = self.TIME_WEATHER_DICT[end_weather_tup]
		weather_end_ind = self.WEATHER_INDEX_DICT[end_cond]

		#location index
		pick_clus = self.KMEANS_1.predict(np.array([pick_lon, pick_lat]).reshape(1,-1))
		drop_clus = self.KMEANS_2.predict(np.array([drop_lon, drop_lat]).reshape(1,-1))
		loc_ind = self.LOCATION_INDEX_DICT[(pick_clus[0], drop_clus[0])]

		#time index
		weekday = datetime.strptime(start_date, '%Y%m%d').weekday()
		weekday_ind = self.WEEK_INDEX.loc[weekday]['index']

		hour_ind = self.HOUR_INDEX.loc[int(start_hour)]['index']

		month_ind = self.MONTH_INDEX.loc[int(month)]['index']

		key = (weather_st_ind, weather_end_ind, loc_ind, weekday_ind, hour_ind, month_ind)

		distance = self.calculate_distance(pick_lat, pick_lon, drop_lat, drop_lat)
		start_time = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
		end_time = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
		time_diff = (end_time - start_time).total_seconds()/60

		value = time_diff/distance

		return key, value


	def clean_raw_ind_data(self, row):		

		ind_1 = row[0].strip("'[")
		ind_2 = row[1].strip(" '")
		ind_3 = row[2].strip(" '")
		ind_4 = row[3].strip(" '")
		ind_5 = row[4].strip(" '")
		ind_6, y = row[5].strip(" '").split(']\t')

		rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
		rv = [float(a) for a in rv]

		return rv


	def get_diff(self, row1, row2):

		rv = [abs(i - j) for i, j in zip(row1, row2)]

		return rv

	def abs_smaller(self, list1, list2):

		for i in range(6):
			if list1[i] > list2[i]:
				return False

		return True

	def is_smallest(self, list1, list2, diff_standard):

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
				for file in self.filenames:
					dir_name = file
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


	def reducer_init(self):

		self.min_diff = [6] * 6
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
		self.KMEANS_1 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array1)
		self.KMEANS_2 = KMeans(n_clusters= 50, 
							 random_state=0).fit(self.coordinate_array2)

		self.WEATHER_DF = pd.read_csv('weather_201507_201606.csv')
		self.WEATHER_DF.columns = ['date', 'hour', 'minute', 'visibility', 'cond']
		self.WEATHER_DF['visibility'].replace(-9999, 10, inplace=True)
		self.WEATHER_DF['date'] = self.WEATHER_DF['date'].astype(str)
		self.WEATHER_DF['hour'] = self.WEATHER_DF['hour'].astype(str)
		self.WEATHER_DF['minute'] = self.WEATHER_DF['minute'].astype(str)
		for ind, row in self.WEATHER_DF.iterrows():
			if row['cond'] == 'Unknown':
				self.WEATHER_DF.loc[ind, 'cond'] = self.WEATHER_DF.iloc[ind - 1]['cond']


		self.HOUR_LIST = []
		self.START = date(2015, 7, 1)
		self.END = date(2016, 6, 30)
		for dt in rrule(HOURLY, dtstart = self.START, until = self.END):
			self.HOUR_LIST.append(dt.strftime("%Y-%m-%d-%H"))

		self.WEATHER_DICT = {}
		for ind, row in self.WEATHER_DF.iterrows():
			key = (row['date'], row['hour'])
			value = (row['cond'], row['visibility'])
			self.WEATHER_DICT[key] = self.WEATHER_DICT.get(key, value)

		self.TIME_WEATHER_DICT = {}
		for time in self.HOUR_LIST:
			time_split = time.split('-')
			day = ''.join(time_split[:3])
			hour = str(int(time_split[-1]))
			key = (day, hour)
			if key not in self.WEATHER_DICT.keys():
				new_key = self.get_last_hour(key)
				while new_key not in self.WEATHER_DICT.keys():
					new_key = self.get_last_hour(new_key)
				self.TIME_WEATHER_DICT[key] = self.WEATHER_DICT[new_key]
			else:
				self.TIME_WEATHER_DICT[key] = self.WEATHER_DICT[key]


		self.WEATHER_INDEX = pd.read_csv('weather_index_time.csv', header=None)
		self.LOCATION_INDEX = pd.read_csv('location_index_time.csv', header=None)
		self.HOUR_INDEX = pd.read_csv('hour_index_time.csv', header=None)
		self.MONTH_INDEX = pd.read_csv('month_index_time.csv', header=None)
		self.WEEK_INDEX = pd.read_csv('weekday_index_time.csv', header=None)

		self.WEATHER_INDEX.columns = ['cond', 'visi', 'index']
		self.WEATHER_INDEX_DICT = {}
		for ind, row in self.WEATHER_INDEX.iterrows():
			key = (row['cond'], row['visi'])
			value = row['index']
			self.WEATHER_INDEX_DICT[key] = value

		self.LOCATION_INDEX.columns = ['pick', 'drop', 'index']
		self.LOCATION_INDEX_DICT = {}
		for ind, row in self.LOCATION_INDEX.iterrows():
			key = (row['pick'], row['drop'])
			value = row['index']
			self.LOCATION_INDEX_DICT[key] = value

		self.HOUR_INDEX.columns = ['hour', 'index']
		self.HOUR_INDEX.set_index('hour')
		self.MONTH_INDEX.columns = ['month', 'index']
		self.MONTH_INDEX.set_index('month')
		self.WEEK_INDEX.columns = ['day', 'index']
		self.WEEK_INDEX.set_index('day')

		# self.filenames = ['raw_2015_08_time.csv']
		self.filenames = ['raw_sample_time.csv']



	def reducer(self, row, lists):

		key, value = self.get_index(row)
		for ind_list in lists:
			result = self.is_smallest(key, ind_list[:6], self.min_diff)
			if result:
				rv = ind_list[6]

		yield row, rv



if __name__ == '__main__':
	MRmatch.run()
