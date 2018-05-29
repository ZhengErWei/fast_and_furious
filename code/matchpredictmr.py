from mrjob.job import MRJob
import mapindex_util
import csv
import sys
sys.path.append('weather_indexing')
import sample


class MRmatch(MRJob):


	def mapper_init(self):

		key, value = mapindex_util.get_ind




	def mapper(self, _, line):

		key, value = mapindex_util.get_index(line)
		with open('')



	def reducer(self, ind, vals):




if __name__ == '__main__':
	MRmatch.run()
