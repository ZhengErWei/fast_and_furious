# Purpose: use this file to find the most similar trip 
#          by comparing the five indices and look at
#          the relationship between pair differences
#  	   of the last index and y variable
# Input: 
# p.s. sample output can be check in ../../data/fix_sample.csv
#      and all files should be put in the same directory


from mrjob.job import MRJob
import csv
import pandas as pd 
from sklearn.cluster import KMeans

class MRcontrol(MRJob):

	def clean_raw_ind_data(self, row):


		ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y = row[1:]

		rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
		rv = [float(a) for a in rv]

		return rv

	def mapper_init(self):

		# hard code the file to find matching pair
		# the datafile should be put inside the same folder
		# you an use raw_time_total_sample.csv and raw_time_total_sample_2.csv
		self.files = ['raw_time_total_sample.csv', 
					 'raw_time_total_sample_2.csv']
		self.vars = ['weather_st_ind', 'weather_end_ind', 'loc_ind', 'weekday_ind', \
					 'hour_ind', 'month_ind']


	def mapper(self, _, line):

		line = next(csv.reader([line]))
		target = self.clean_raw_ind_data(line)
		for file in self.files:
			with open(file, 'r') as f:
				rb = csv.reader(f)
				for row in rb:
					try:
						ind = self.clean_raw_ind_data(row)
						diff = [i - j for i, j in zip(target[:6], ind[:6])]
						rv = diff + [ind[6]]

						key = tuple(target)
						
						yield key, tuple(rv)

					except:
						key = None



	def reducer_init(self):

		self.vars = ['weather_st_ind', 'weather_end_ind', 'loc_ind', \
					 'weekday_ind', 'hour_ind', 'month_ind']


	def abs_smaller(self, list1, list2):
		'''
		To decide whether all value in list1 is smaller than list 2
		'''

		for i in range(5):
			if list1[i] > list2[i]:
				return False

		return True


	def reducer(self, line, diff_tup):

		target_ind_1, target_ind_2, target_ind_3, target_ind_4, \
		target_ind_5, target_ind_6, target_val = line
		for i in range(6):
			min_diff = [6, 6, 6, 6, 6]

			ind_list = [0, 1, 2, 3, 4, 5, 6]
			ind_rest = list(set(ind_list) - set([i]))


			for tup in diff_tup:
				tup_list = [list(tup)[i] for i in ind_rest]
				if self.abs_smaller(tup_list, min_diff):
					min_diff = tup_list
					rv_diff = tup[i]
					rv_val = tup[6]

			label = self.vars[i]

			value = (line[i], target_val, line[i] - rv_diff, rv_val)
			
			yield label, value



if __name__ == '__main__':
	MRcontrol.run()
	
















