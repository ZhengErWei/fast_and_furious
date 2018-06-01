from mrjob.job import MRJob
import csv
import pandas as pd 
from sklearn.cluster import KMeans

class MRcontrol(MRJob):

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

	def mapper_init(self):

		# self.files = ['raw_2015_07_time.csv']
		self.files = ['Book2.csv']
		# self.files = ['raw_sample_time.txt']
		self.vars = ['weather_st_ind', 'weather_end_ind', 'loc_ind', 'weekday_ind', 'hour_ind', 'month_ind']


	def mapper(self, _, line):

		try:	
			line = next(csv.reader([line]))
			target = self.clean_raw_ind_data(line)
			# target_ind_1, target_ind_2, target_ind_3, target_ind_4, target_ind_5, target_ind_6, target_val  = target
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
		except:
			key = None


	def reducer_init(self):

		self.vars = ['weather_st_ind', 'weather_end_ind', 'loc_ind', 'weekday_ind', 'hour_ind', 'month_ind']


	def abs_smaller(self, list1, list2):

		for i in range(5):
			if list1[i] > list2[i]:
				return False

		return True


	def reducer(self, line, diff_tup):

		target_ind_1, target_ind_2, target_ind_3, target_ind_4, target_ind_5, target_ind_6, target_val = line
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
	
















