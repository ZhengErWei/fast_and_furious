# Purpose: use this file to get sample file of given size from all taxi dataset
#          or index dataset
# command: python3 thisfile


import pandas as pd 

# you can hard code here
DIR = '../../data/'



def draw_one_sample(filename):

	dirname = DIR + filename
	df = pd.read_csv(dirname)
	df = df.dropna(how='any')
	## for index file
	df.iloc[:, 0] = df.iloc[:, 0].str.strip('[')
	a = df.iloc[:,5].str.split(']\t',1).str[0]
	b = df.iloc[:,5].str.split(']\t',1).str[1]
	df_new = pd.concat([df.iloc[:,0], df.iloc[:,1], df.iloc[:,2], df.iloc[:,3], \
						df.iloc[:,4], a, b], axis = 1, ignore_index = True)

	sample_df = df_new.sample(n = 2000000, random_state=23)

	return sample_df

# taxi data names
# FILENAMES = ['yellow_tripdata_2015-07.csv', 'yellow_tripdata_2015-08.csv', \
# 			 'yellow_tripdata_2015-09.csv', 'yellow_tripdata_2015-10.csv', \
# 			 'yellow_tripdata_2015-11.csv', 'yellow_tripdata_2015-12.csv', \
# 			 'yellow_tripdata_2016-01.csv', 'yellow_tripdata_2016-02.csv', \
# 			 'yellow_tripdata_2016-03.csv', 'yellow_tripdata_2016-04.csv', \
# 			 'yellow_tripdata_2016-05.csv', 'yellow_tripdata_2016-06.csv']

FILENAMES = ['raw_2015_07_time.csv', 'raw_2015_08_time.csv', 'raw_2015_09_time.csv', \
			'raw_2015_10_time.csv', 'raw_2015_11_time.csv']


if __name__ == '__main__':
	df_list = []
	for file in FILENAMES:
		df_list.append(draw_one_sample(file))
	rv = pd.concat(df_list)

	# you can hard code here
	rv.to_csv('../../data/raw_time_total_1.csv')







