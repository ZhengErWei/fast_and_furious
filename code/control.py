from mrjob.job import MRJob
import math
import csv
import numpy as np
import pandas as pd

# index_col = dict(weather_st_ind = 0, weather_end_ind = 1, loc_ind = 2, 
# 				 weekday_ind = 3, hour_ind = 4, month_ind = 5)

class MRindex(MRJob):

	def mapper(self, _, line):
		temp = line.split(']')
		try:
			y = round(float(temp[1]), 4)
			index_list = temp[0].strip('[')
			index_together = [float(l) for l in index_list.split(',')]
			total = round(np.sum(index_together) - index_together[i],4)
			# index_sum = [round( total - index_together[i], 2) for i in range(6)]
			# print ((round(index_together[i], 4), total), y)
			yield ((round(index_together[i], 4), total), y)

		except:
			pass


	# def combiner(self, key, value):
	# 	yield (key, value), count(value)


	# def reducer(self, key, value):

	# 	total_counts = sum(counts)

	# 	if total_counts >= 10:
	# 		yield None, name


if __name__ == '__main__':

	for i in range(0,6):
		MRindex.run()
