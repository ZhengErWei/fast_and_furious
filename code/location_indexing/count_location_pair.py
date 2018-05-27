import csv
from mrjob.job import MRJob

class MRlocation(MRJob):

	def mapper(self, _, line):

		row = next(csv.reader([line]))
		if len(row) > 0:
			location = (row[7], row[8])

			yield location, 1

	def combiner(self, loc, counts):

		yield loc, sum(counts)

	def reducer(self, loc, counts):

		yield loc, sum(counts)

if __name__ == '__main__':
	MRlocation.run()
