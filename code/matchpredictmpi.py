from mpi4py import MPI 
import csv
import mapindex_util
import numpy as np
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


def clean_raw_ind_data(row):

	# ind_1 = row[0].strip("'[")
	# ind_2 = row[1].strip(" '")
	# ind_3 = row[2].strip(" '")
	# ind_4 = row[3].strip(" '")
	# ind_5 = row[4].strip(" '")
	# ind_6, y = row[5].strip(" '").split(']\t')

	ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y = row[1:]

	rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
	rv = [float(a) for a in rv]

	return rv


# scatter
def get_the_best_fit(target_row):

	key, value = mapindex_util.get_index(target_row)

	if rank == 0:
		# filenames = ['raw_2015_07_time.csv', 'raw_2015_08_time.csv', 
		# 			 'raw_2015_09_time.csv', 'raw_2015_10_time.csv', 
		# 			 'raw_2015_11_time.csv', 'raw_2015_12_time.csv',
		# 			 'raw_2016_01_time.csv', 'raw_2016_02_time.csv',
		# 			 'raw_2016_03_time.csv', 'raw_2016_04_time.csv', 
		# 			 'raw_2016_05_time.csv', 'raw_2016_06_time.csv']
		filenames = ['raw_2015_07_time.csv']
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
	inputfile, outputfile = sys.argv[1:]
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


