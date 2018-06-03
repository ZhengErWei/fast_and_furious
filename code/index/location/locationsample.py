import pandas as pd 

COLUMNS = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
           'passenger_count', 'trip_distance', 'pickup_longitude',
           'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag',
           'dropoff_longitude', 'dropoff_latitude', 'payment_type', 
           'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
           'improvement_surcharge', 'total_amount']

 def draw_one_sample(filename):

    df = pd.read_csv(filename)
    df = df.dropna(how='any')
    sample_df = df.sample(n = 1000)
    df_remain = df.drop(sample_df.index)
    return sample_df, df_remain


FILENAMES = ['yellow_tripdata_2015-07.csv', 'yellow_tripdata_2015-08.csv', 
             'yellow_tripdata_2015-09.csv', 'yellow_tripdata_2015-10.csv', 
             'yellow_tripdata_2015-11.csv', 'yellow_tripdata_2015-12.csv', 
             'yellow_tripdata_2016-01.csv', 'yellow_tripdata_2016-02.csv',  
             'yellow_tripdata_2016-03.csv', 'yellow_tripdata_2016-04.csv', 
             'yellow_tripdata_2016-05.csv', 'yellow_tripdata_2016-06.csv']
             

if __name__ == '__main__':
	df_list = []
	df_list2 = []
	for file in FILENAMES:
		sample_df, df_remain = draw_one_sample(file)
		df_list.append(sample_df)
		df_list2.append(df_remain)
	rv1 = pd.concat(df_list)
    rv2 = pd.concat(df_list2)
    rv1.to_csv('sample_trip.csv')
    rv2.to_csv('nonsample_trip.csv')
