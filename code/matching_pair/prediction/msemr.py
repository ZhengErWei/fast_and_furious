# command: python3 reg_result_compare.py --jobconf mapreduce.job.reduces=1 sample_match_time.csv 
# purpose: use this function to get mse of prediction made by matching pair and dummy regressor by mean
# Input: ../../../data/match_mr.txt

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from math import sin, cos, sqrt, atan2, radians
from datetime import datetime, date, timedelta


class MRpairreg(MRJob):
	
	# this funciton copied from stackflow
	def calculate_distance(self, lat1, lon1, lat2, lon2):
	# approximate radius of earth in km
		R = 6373.0

		dlon = radians(lon2) - radians(lon1)
		dlat = radians(lat2) - radians(lat1)

		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
		c = 2 * atan2(sqrt(a), sqrt(1 - a))

		distance = R * c

		return distance

	# when y is traffic time, clean the data and get the value
	def get_index(self, row):

		pick_lon = float(row[6].strip(' "'))
		pick_lat = float(row[7].strip(' "'))
		drop_lon = float(row[10].strip(' "'))
		drop_lat = float(row[11].strip(' "'))

		distance = self.calculate_distance(pick_lat, pick_lon, drop_lat, drop_lat)
		start_time = datetime.strptime(row[2].strip(' "'), '%Y-%m-%d %H:%M:%S')
		end_time = datetime.strptime(row[3].strip(' "'), '%Y-%m-%d %H:%M:%S')
		time_diff = (end_time - start_time).total_seconds()/60
		value = time_diff/distance

		return value


	def clean_output(self, row):
		
		y_true = self.get_index(row)
		_, y_pred  = row[-1].strip(" '").split(']\t')
		# if match_mpi.txt
		# y_pred = row[-1]

		return float(y_pred), y_true


	def mapper(self, _, line):
		
		row = next(csv.reader([line])) 
		y_pred, y_true = self.clean_output(row)

		yield y_pred, y_true


	def reducer_first_init(self):

		self.count = 0
		self.y_sum = 0
		self.y_mean = 0
		self.data_dict = {}


	def reducer_first(self, key, vals):
		y_pred = key
		values = vals

		for y_true in values:
			self.y_sum += y_true
			self.count += 1

			if y_true not in self.data_dict:
				self.data_dict[y_true] = []
			self.data_dict[y_true].append(y_pred)


	def reducer_first_final(self):

		self.y_mean = self.y_sum / self.count
		
		for y_pred, vals in self.data_dict.items():
			for value in vals:
				yield (y_pred, value), self.y_mean


	def reducer_second_init(self):

		self.count = 0
		self.sum_pred = 0
		self.sum_mean = 0
		self.mse_pred = 0
		self.mse_mean = 0


	def reducer_second(self, key, vals):

		y_pred = key[0]
		y_true = key[1]
		for v in vals:
			y_mean = v

		self.count += 1
		

		self.sum_pred += (y_pred - y_true) ** 2
		self.sum_mean += (y_mean - y_true) ** 2


	def reducer_second_final(self):

		self.mse_pred = self.sum_pred / self.count
		self.mse_mean = self.sum_mean / self.count

		yield self.mse_pred, self.mse_mean


	def steps(self):

		return [MRStep(mapper = self.mapper, 
					   reducer_init = self.reducer_first_init,
					   reducer = self.reducer_first,
					   reducer_final = self.reducer_first_final),

				MRStep(reducer_init = self.reducer_second_init,
					   reducer = self.reducer_second,
					   reducer_final = self.reducer_second_final)]


if __name__ == '__main__':
	MRpairreg.run()


