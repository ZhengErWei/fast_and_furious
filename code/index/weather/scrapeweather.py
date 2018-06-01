from WunderWeather import weather
import arrow
import csv
from datetime import date, timedelta

KEY = '534fe8de0c6c00ce'
LOCATION = 'NY/New York'
EXTRACTOR = weather.Extract(KEY)

COLUMNS = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       	   'passenger_count', 'trip_distance', 'pickup_longitude',
           'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag',
           'dropoff_longitude', 'dropoff_latitude', 'payment_type', 
           'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
           'improvement_surcharge', 'total_amount']


def get_all_dates(start_y, start_m, start_d, end_y, end_m, end_d):
	'''
	Input:
	  e.g. 2017, 1, 1, 2017, 12, 31

	Return: a list
	'''

	d1 = date(start_y, start_m, start_d)
	d2 = date(end_y, end_m, end_d)
	delta = d2 - d1
	date_list = []
	for i in range(delta.days + 1):
		rv = d1 + timedelta(days=i)
		rv = rv.strftime('%Y%m%d')
		date_list.append(rv)

	return date_list



def get_weather(date):
	'''
	date : e.g. '20170801'

	data you can get: 'date', 'utcdate', 'tempm', 'tempi', 'dewptm', 
	'dewpti', 'hum', 'wspdm', 'wspdi', 'wgustm', 'wgusti', 'wdird', 
	'wdire', 'vism', 'visi', 'pressurem', 'pressurei', 'windchillm', 
	'windchilli', 'heatindexm', 'heatindexi', 'precipm', 'precipi', 
	'conds', 'icon', 'fog', 'rain', 'snow', 'hail', 'thunder', 'tornado', 
	'metar'
	'''

	d =  arrow.get(date, 'YYYYMMDD')
	response = EXTRACTOR.date(LOCATION, d.format('YYYYMMDD'))
	obs_list = response.data.observations
	for obs in obs_list:
		hour = obs['date']['hour']
		minute = obs['date']['min']
		visi = obs['visi']
		rain = obs['rain']
		snow = obs['snow']
		conds = obs['conds']
		data_list = [date, hour, minute, visi, rain, snow, conds]
		with open('../data/weather_2015_7.csv', 'a') as f:
			wb = csv.writer(f, delimiter = ',')
			wb.writerow(data_list)

	return 


if __name__ == '__main__':
	date_list = get_all_dates(2015, 7, 1, 2016, 6, 30) 
	for date in date_list:
		get_weather(date)



