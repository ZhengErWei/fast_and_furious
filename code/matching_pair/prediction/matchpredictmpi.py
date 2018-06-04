# Purpose: To get prediction of each row by looking at the sum of squared difference of the six index
# command: python3 thisfile
# p.s. mapindex_util.py, mapweather_util.py and all files imported from these files should be put in the same folder
# Input: files can be found in .../../../data (sample_trip_2.csv, raw_time_sample.csv)

from mpi4py import MPI 
import csv
import mapindex_util
import numpy as np
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def clean_raw_ind_data(row):

	ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y = row[1:]

	rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
	rv = [float(a) for a in rv]

	return rv

# scatter
def get_the_best_fit(target_row):
	'''
	Get the most similar trip after looping over the two files
	'''
	
	# here use try because there is some clusters not showing up in 
	# sample trainig file
	try:
		key, value = mapindex_util.get_index(target_row)

		if rank == 0:
			files = ['raw_time_sample.csv']
# 			filenames = ['raw_time_total_sample.csv', 
# 				     'raw_time_total_sample_2.csv']
			chunks = np.array_split(filenames, size)
		else:
			chunks = None


		chunk = comm.scatter(chunks, root=0) 


		min_diff = 6
		for filename in chunk:
			with open(filename, 'r') as f:
				rb = csv.reader(f)
				for row in rb:
					sse = 0
					ind = clean_raw_ind_data(row)
					for i in range(6):
						sse += (ind[i] - key[i]) ** 2
					if sse < min_diff:
						pred = ind[6]
						min_diff = sse
			
		results = comm.gather((min_diff, pred), root=0)
		if rank == 0:
			target = list(np.concatenate((results)))

			if target[0] >= target[2]:
				return target[3]
			else:
				return target[1]
		else:
			return 
	except:
		return None


if __name__ == '__main__':
	inputfile = 'sample_trip_2.csv'
	outputfile = 'match_mpi.csv'
	with open(inputfile, 'r') as f:
		rb = csv.reader(f)
		next(f)
		for row in rb:
			pred = get_the_best_fit(row)
			if pred != None:
				rv = row + [pred]
				with open(outputfile, 'a') as f2:
					wb = csv.writer(f2, delimiter=',')
					wb.writerow(rv)

 
