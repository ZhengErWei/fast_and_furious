import math
import csv
from mrjob.job import MRJob
import sys

class MRcount(MRJob):

	def mapper(self, _, line):

		row = next(csv.reader([line]))
		location = (row[2], row[3])
		if row[2] != 'pickup_cluster':
			yield location, 1

	def combiner(self, location, counts):

		yield location, sum(counts)

	def reducer(self, location, counts):

		yield location, sum(counts)


class MRmeans(MRJob):

	def mapper(self, _, line):

		row = next(csv.reader([line]))
		location = (row[2], row[3])
		val = row[1]
		if row[2] != 'pickup_cluster':
			yield location, val

	def reducer_init(self):
		self.location_dict = {}
		self.max = 0
		self.min = 0


	def reducer(self, location, ys):

		y_sum = 0
		y_ct = 0
		for val in ys:
			val = float(val)
			y_ct += 1
			y_sum += val
			if val > self.max:
				self.max = val
			if val < self.min:
				self.min = val

		self. location_dict[tuple(location)] = y_sum/y_ct

	def reducer_final(self):

		lengh = self.max - self.min
		for key, value in self.location_dict.items():
			yield key, value/lenth
		yield self.max, self.min

if __name__ == '__main__':

	MRmeans.run()
