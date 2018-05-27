from mrjob.job import MRJob
import sys
sys.path.append('weather_indexing')
import mapweather_util
import csv
import pandas as pd

TIME_WEATHER_DICT = mapweather_util.get_weather_hour_dict()
WEATHER_INDEX = pd.read_csv('../data/weather_index/weather_index_time.csv')
LOCATION_INDEX = pd.read_csv('../data/location_index/location_index_time.csv')


class MRindex(MRJob):

	def mapper(self, _, line):

		row = next(csv.reader([line]))
		

	def resucer(self, ind, vals):

		for weather in vals:
			
			yield ind, weather

if __name__ == '__main__':
	MRweather.run()