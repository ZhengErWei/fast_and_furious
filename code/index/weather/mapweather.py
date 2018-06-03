# map weather condition to the sample dataset/large dataset using mpi
# can use file in ../../../data/sample.csv as the first input

import mapweather_util
from mpi4py import MPI 
import sys
import pandas as pd
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def get_weather_chunk(filename):

	df = pd.read_csv(filename)
	time_df = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']]
	time_df = time_df.sort_values(by = ['tpep_pickup_datetime', \
										'tpep_dropoff_datetime'])
	start_t = time_df.iloc[0, 0][:10]
	end_t = time_df.iloc[-1, 1][:10]
	weather_df = mapweather_util.get_weather_df(start_t, end_t)

	return weather_df


if __name__ == '__main__':
	# sample_file is the input file, op_file is the output file
	sample_file, op_file = sys.argv[1:]
	weather_df = get_weather_chunk(sample_file)
	if rank == 0:
		# weather_df = get_weather_chunk(sample_file)
		sample_df = pd.read_csv(sample_file)
		sample_df_time = sample_df[['tpep_pickup_datetime', \
									'tpep_dropoff_datetime']]

		# to make it efficient, we can sort date before processing
		# sample_df_time = sample_df_time.sort_values(by = \
		# 				['tpep_pickup_datetime', 
		# 				'tpep_dropoff_datetime'])


		chunks = np.split(sample_df_time, size)
	else:
		chunks = None

	chunk = comm.scatter(chunks, root = 0)
	rv = mapweather_util.get_all_weather(chunk, weather_df)
	rv_all = pd.concat([chunk, rv], axis = 1)

	gathered_dfs = comm.gather(rv, root = 0)
	total_df = pd.concat(gathered_dfs)
	total_df.to_csv(op_file)



