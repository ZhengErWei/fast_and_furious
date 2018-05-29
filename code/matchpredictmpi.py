from mpi4py import MPI 
import csv
import mapindex_util
import clean_output
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


# scatter
def get_the_best_fit(target_row):

	key, value = mapindex_util.get_index(target_row)
	key_sum = sum(key)

	if rank == 0:
		filenames = ['raw_2015_07_time.csv', 'raw_2015_08_time.csv', 
					 'raw_2015_09_time.csv', 'raw_2015_10_time.csv', 
					 'raw_2015_11_time.csv', 'raw_2015_12_time.csv',
					 'raw_2016_01_time.csv', 'raw_2016_02_time.csv',
					 'raw_2016_03_time.csv', 'raw_2016_04_time.csv', 
					 'raw_2016_05_time.csv', 'raw_2016_06_time.csv']
	    
	    chunks = np.array_split(filenames, size)
	else:
	    chunks = None


	chunk = comm.scatter(chunks, root=0) 


	min_diff = 6
	for filename in chunk:
		with open(filename, 'r') as f:
			rb = csv.reader(f)
			for row in rb:
				try:
					ind, y = clean_output.clean_raw_ind_data(row)
					ind_sum = sum([float(i) for i in ind])
					y = float(y)
					diff = abs(key_sum - ind_sum)
					if diff < min_diff:
						pred = y
				except:
					continue

	results = comm.gather(pred, root=0)
	rv = results[np.abs(results - value).argmin()]

	return rv

if __name__ == '__main__':
	inputfile, outputfile = sys.argv[1:]
	with open(inputfile, 'r') as f:
		rb = csv.reader(f)
		next(f)
		for row in rb:
			pred = get_the_best_fit(row)
			rv = row + pred
			with open(outputfile, 'a') as f2:
				wb = csv.writer(f2, delimiter=',')
				wb.writerow(rv)


