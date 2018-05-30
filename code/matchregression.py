from mrjob.job import MRJob
from mrjob.step import MRStep
import math
import csv


class MRindex(MRJob):


	def mapper_init():
		self.count = [0] * 6
		self.x_mean = [0] * 6
		self.y_mean = [0] * 6
		self.x_sum = [0] * 6
		self.y_sum = [0] * 6
		self.beta_1 = [0] * 6
		self.beta_0 = [0] * 6

		self.convert = dict(weather_st_ind:1, weather_end_ind:2, loc_ind:3, 
							weekday_ind:4, hour_ind:5, month_ind: 6)


	def mapper(self, _, line):
		#need to change
		key, original, compare = line.split(')') 

		yield (key, (original, compare))


	def reducer_init(self, key, value):
		yield (key, value)


	def reducer_first(self, key, value):
		
		index = self.convert[key]
		original, compare = value

		x_diff = original[0] - compare[0]
		y_diff = original[1] - compare[1]

		self.x_sum[index] += x_diff
		self.y_sum[index] += y_diff
		self.count[index] += 1

		yield (index, (x_diff, y_diff))


	def reducer_second(self):

		self.x_mean = [a/b for a, b in zip(self.x_sum, self.count)]
		self.y_mean = [a/b for a, b in zip(self.y_sum, self.count)]


	def reducer_sum(self, index, value):

		x_diff, y_diff = value

		top = (x_diff - self.x_mean[index]) * (y_diff - self.y_mean[index])
		bottom = math.power((x_diff - self.x_mean[index]),2)

		self.beta_1[index] += top / bottom


	def reducer_final(self):
		self.beta_0 = [a - b * c for a, b, c in zip(self.y_mean, self.x_mean, self.beta_1)]

		yield (self.beta_0, self.beta_1)


	def steps(self):
    	return [MRStep(mapper_init = self.mapper_init, 
    				   mapper = self.mapper, 
    				   reducer = self.reducer_init),
      			MRStep(reducer_init = self.reducer_first,
      				   reducer = self.reducer_second,
      				   reducer_final = self.reducer_final]




