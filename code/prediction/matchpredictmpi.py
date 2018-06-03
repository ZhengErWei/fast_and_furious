# run this file to get prediction of each row by looking 
# at the sum of squared difference of the six index
# command: python3 thisfile
# mapindex_util.py, mapweather_util.py and all files 
# imported from these files should be put in the same folder

from mpi4py import MPI 
import csv
import mapindex_util
import numpy as np
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


def clean_raw_ind_data(row):
	'''
	Use the function to clean output
	'''

	ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y = row[1:]

	rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
	rv = [float(a) for a in rv]

	return rv


# scatter
def get_the_best_fit(target_row):
	'''
	To get the best match by looking through 
	all the rows in the given files
	'''

	key, value = mapindex_util.get_index(target_row)

	if rank == 0:
		# hard code the file name
		# these two files can be checked in ../../data
		filenames = ['raw_time_total_sample.csv', 'raw_time_total_sample_2.csv']
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
				try:
					ind = clean_output.clean_raw_ind_data(row)
					for i in range(6):
						sse += (ind[i] - key[i]) ** 2
					if sse < min_diff:
						pred = ind[6]
						min_diff = sse
				except:
					continue

	results = comm.gather((sse, pred), root=0)
	rv = min(results, key = lambda t: t[0])[1]

	return rv

if __name__ == '__main__':
	inputfile = 'sample_trip_2.csv'
	outputfile = 'match_mpi.csv'
	with open(inputfile, 'r') as f:
		rb = csv.reader(f)
		next(f)
		for row in rb:
			try:
				pred = get_the_best_fit(row)
				rv = row + [pred]
				with open(outputfile, 'a') as f2:
					wb = csv.writer(f2, delimiter=',')
					wb.writerow(rv)
			except:
				continue


