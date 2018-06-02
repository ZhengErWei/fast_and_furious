# Ideas are motivated by
# http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/
# but the codes are original.

import re
import pandas as pd
import datetime as dt
import networkx as nx


EPSILON = 0.001

INDICES = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
           'passenger_count', 'trip_distance', 'pickup_longitude', \
           'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag', \
           'dropoff_longitude', 'dropoff_latitude', 'payment_type', \
           'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', \
           'improvement_surcharge', 'total_amount', 'label']

CLUBS = {"Avenue": (40.74405065, -74.00631335),
         "Bembe": (40.7111048, -73.9651604),
         "Black Flamingo": (40.7104806, -73.9540968),
         "C'mon Everybody": (40.6883159, -73.9568761),
         "Cielo": (40.7397342, -74.0070218),
         "Drink Lounge": (40.6721649, -73.9576687),
         "Electric Room": (40.7423311, -74.0034791),
         "FREQ": (40.766805, -73.996215),
         "Friends and Lovers": (40.678534, -73.958435),
         "Good Room": (40.7268963, -73.9529488),
         "Grill On The Hill": (40.8222947, -73.9501033),
         "House of Yes": (40.706758, -73.92355685),
         "Hudson Terrace": (40.7640706, -73.9975569),
         "Le Bain": (40.7409232, -74.008111),
         "Littlefield": (40.6785007, -73.9833014),
         "Marquee New York": (40.7500697, -74.0027905),
         "Minton's Playhouse": (40.8046775, -73.9522994): ,
         "Nublu": (40.722524, -73.9797307),
         "Output": (40.7223166, -73.9578179): ,
         "The Jane Hotel": (40.7382579, -74.0094507),
         "The Press Lounge": (40.7645758, -73.99595),
         "The Rosemont": (40.7071318, -73.9474399),
         "Verboten": (40.7220265, -73.9591725)}


def data_cleaning(df):
    '''
    Returns dataframe after removing unusual trip.
    '''
    df = df[df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"]]
    df = df[df["pickup_longitude"] != 0.0]
    df = df[df["pickup_longitude"] != 0.0]
    df = df[df["dropoff_longitude"] != 0.0]
    df = df[df["dropoff_latitude"] != 0.0]
    df = df[df["fare_amount"] > 3.30]
    df = df[df["tip_amount"] > 0.0]

    df["tip_rate"] = df["tip_amount"] / df["fare_amount"]
    df["my_dates"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["year"] = df["my_dates"].dt.year
    df["month"] = df["my_dates"].dt.month
    df["day"] = df["my_dates"].dt.day
    df["day_of_week"] = df["my_dates"].dt.weekday_name
    df["time"] = df["my_dates"].dt.time
    df["hour"] = df["my_dates"].dt.hour
    df["minute"] = df["my_dates"].dt.minute

    return df


def newark_pickup(df):
    '''
    Returns trip that is neighboring Newark Airport.
    '''
    rv = df[(((df["pickup_longitude"] < -74.186) | \
              (df["pickup_longitude"] > -74.175)) & \
              ((df["pickup_latitude"] < 40.687) | \
              (df["pickup_latitude"] > 40.697))) & \
            (((df["dropoff_longitude"] >= -74.186) & \
              (df["dropoff_longitude"] <= -74.175)) & \
              ((df["dropoff_latitude"] >= 40.687) & \
              (df["dropoff_latitude"] <= 40.697)))]

    rv = rv[(rv["RatecodeID"] != 5) & (rv["trip_distance"] > 5) & \
            (rv["tip_rate"] > 0.5)]
    
    return rv


def type_casting(df):
    '''
    Returns dataframe after removing quotation and type casting to float.
    '''
    for i in INDICES:
        df[i] = df[i].apply(lambda x: re.sub('\"', '', x))

        if i in ['tpep_pickup_datetime', 'tpep_dropoff_datetime', \
                 'store_and_fwd_flag', 'label']:
            df[i].astype(str)
        else:
            df[i].astype(float)

    return df


def get_network(input_file):
    '''
    Returns a network of trips in the input file.
    '''
    df = pd.read_csv(input_file, index_col="Unnamed: 0")
    G = nx.Graph()
    stations = set()

    for idx, row in df.iterrows():
        origin = (row["pickup_longitude"], row["pickup_latitude"])
        destination = (row["dropoff_longitude"], row["dropoff_latitude"])
        
        if (-74.06 <= origin[0] <= -73.77) & \
        (-74.06 <= destination[0] <= -73.77) & \
        (40.61 <= origin[1] <= 40.91) & \
        (40.61 <= destination[1] <= 40.91):
            stations.add(origin)
            stations.add(destination)

    diction = {}
    c = 0
    for i in stations:
        c += 1
        diction[i] = c

    for k, v in diction.items():
        G.add_node(v, lat=k[0], long=k[1])

    for idx, row in df.iterrows():
    origin = (row["pickup_longitude"], row["pickup_latitude"])
    destination = (row["dropoff_longitude"], row["dropoff_latitude"])
    
        if (-74.06 <= origin[0] <= -73.77) & \
        (-74.06 <= destination[0] <= -73.77) & \
        (40.61 <= origin[1] <= 40.91) & \
        (40.61 <= destination[1] <= 40.91):
            G.add_edge(diction[origin], diction[destination])

    return G


def pickup_neighbor(df, coordi):
    '''
    Returns index of pickup location that is neighboring the coordinate.
    '''
    lati, longi = coordi

    rv = df[(df["pickup_latitude"] > lati - EPSILON) & \
    	(df["pickup_latitude"] < lati + EPSILON) & \
    	(df["pickup_longitude"] > longi - EPSILON) & \
    	(df["pickup_longitude"] < longi + EPSILON)].index

    return rv


def dropoff_neighbor(df, coordi):
    '''
    Returns index of dropoff location that is neighboring the coordinate.
    '''
    lati, longi = coordi

    rv = df[(df["dropoff_latitude"] > lati - EPSILON) & 
    	(df["dropoff_latitude"] < lati + EPSILON) & \
    	(df["dropoff_longitude"] > longi - EPSILON) & \
    	(df["dropoff_longitude"] < longi + EPSILON)].index

    return rv


def get_nightclub_network(input_file):
    '''
    Returns a network between known nightclubs.
    '''
    df = pd.read_csv(input_file, index_col="Unnamed: 0")

    df = data_cleaning(df)

    nightclub = df[((df["day_of_week"] == "Friday") | \
                    (df["day_of_week"] == "Saturday") | \
                    (df["day_of_week"] == "Sunday")) & \
                    (((df["time"] >= dt.time(0, 0, 0)) & \
                    (df["time"] < dt.time(4, 30, 0))))]
    nightclub["start"] = 0
    nightclub["end"] = 0

    c = 0
    for club in CLUBS.values():
        c += 1

        nightclub.loc[pickup_neighbor(nightclub, club), "start"] = c
        nightclub.loc[dropoff_neighbor(nightclub, club), "end"] = c

    unweighted = nightclub[(nightclub["start"] != 0) & \
                            (nightclub["end"] != 0) & \
                            (nightclub["start"] != nightclub["end"])]

    G = nx.from_pandas_dataframe(unweighted, "start", "end")
    
    return G


def combine_dataframe():
    '''
    Returns combined dataframe from 2015/01 to 2016/06.
    '''
    lst = []
    for y in range(2015, 2017):
        for m in range(1,13):
            if y == 2016 and m > 6:
                continue
            lst.append(str(y) + "-" + ("0" + str(m) if m < 10 else str(m)))

    for t in lst:    
        if t == "2009-01":
            rv = pd.read_csv("de_anonymization_2015-01.csv", header=None, \
                              names=INDICES, index_col="Unnamed: 0")
        else:
            temp = pd.read_csv("de_anonymization_" + t + ".csv", header=None,\
                               names=INDICES, index_col="Unnamed: 0")
            rv = pd.concat([rv, temp], axis = 0)

    return rv


def time_series(flag):
    '''
    Returns time series dataframe of counts of known locations.
    '''
    mapping = {"Bank": ["Citigroup", "Goldman Sachs"], \
               "Club": CLUBS.keys()}

    if flag == "Bank":
        rng = pd.date_range(start='2015/01 00:00', \
                            end="2015/01 23:59", freq='1min')

    elif flag == "Club":
        rng = pd.date_range(start='2015/01 00:00', \
                            end="2015/01 4:29", freq='1min')

    lst = []
    for i in pd.Series(index=rng).index:
        lst.append(i)

    ts = pd.DataFrame(index=lst)

    for place in mapping[flag]:
        ts[place] = 0

        for idx, row in ts.iterrows():
            try:
                temp = pd.read_csv(place + ".csv", header=None, \
                                   index_col="Unnamed: 0")
            except:
                continue

            try:
                ts.loc[idx, place] = int(temp[(temp[0] == idx.hour) & \
                                        (temp[1] == idx.minute)][2].values)
            except:
                continue

        if flag == "Bank":
            ts[place] = ts[place] / ts[place].max()

        elif flag == "Club":
            ts[place] = ts[place]

    return ts


def find_pair(input_file):
    '''
    Returns pair of trips that have opposite pickup and dropoff location.
    '''
    df = pd.read_csv(input_file, index_col="Unnamed: 0")

    df = data_cleaning(df)
    df = df[(df["pickup_longitude"] != df["dropoff_longitude"]) | \
            (df["pickup_latitude"] != df["dropoff_latitude"])]
    df = df.round(3)
    df = df.groupby(["year", "month", "day", "tip_amount", "label"]\
                    ).filter(lambda x: len(x) == 2)
    df = df.sort_values(["label", "tip_amount", "year", "month", "day"]\
                        ).reset_index()

    rv = pd.DataFrame(columns=df.columns)
    for idx, row in df.iterrows():
        if idx % 2 == 0:
            next_row = df.iloc[idx+1]

            if (((row["pickup_longitude"] == next_row["dropoff_longitude"]) & \
                 (row["pickup_latitude"] == next_row["dropoff_latitude"])) | \
                ((row["dropoff_longitude"] == next_row["pickup_longitude"]) & \
                 (row["dropoff_latitude"] == next_row["pickup_latitude"]))):
                rv.append(row)
                rv.append(next_row)

    return rv
