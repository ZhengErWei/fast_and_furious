# python3 filename --jobconf mapreduce.job.reduces=1 inputfile > outputfile

import math
import csv
from mrjob.job import MRJob
import sys

class MRcount(MRJob):

	def mapper(self, _, line):

		row = next(csv.reader([line]))
		weather = (row[2], row[3])
		if row[2] != 's_cond':
			yield weather, 1

	def combiner(self, weather, counts):

		yield weather, sum(counts)

	def reducer(self, weather, counts):

		yield weather, sum(counts)



class MRmeans(MRJob):

	def mapper(self, _, line):
	
		row = next(csv.reader([line]))
		weather = (row[2], row[3])
		val = row[1]
		if row[2] != 's_cond':
			yield weather, val
	

	def reducer_init(self):

		self.weather_dict = {}
		self.max = 0
		self.min = 0


	# def reducer(self, weather, ys):

	# 	y_sum = 0
	# 	y_ct = 0
	# 	for val in ys:
	# 		val = float(val)
	# 		y_ct += 1
	# 		y_sum += val

	# 	self.weather_dict[tuple(weather)] = y_sum/y_ct

	# def reducer_final(self):

	# 	sort_list = sorted(self.weather_dict.items(), key=lambda t: t[1])
	# 	for data in sort_list:
	# 		yield data[0], data[1]

	def reducer(self, weather, ys):

		y_sum = 0
		y_ct = 0
		for val in ys:
			val = float(val)
			# if val <= 100:
			if val <= 1:
				y_ct += 1
				y_sum += val
				if val > self.max:
					self.max = val
				if val < self.min:
					self.min = val

		self.weather_dict[tuple(weather)] = y_sum/y_ct

	def reducer_final(self):

		# sort_list = sorted(self.weather_dict.items(), key=lambda t: t[1])
		# for data in sort_list:
		# 	yield data[0], data[1]
		length = self.max - self.min
		for key, value in self.weather_dict.items():
			yield key, (value - self.min)/length
		# yield self.max, self.min


if __name__ == '__main__':
# 	MRcount.run()
	MRmeans.run()

