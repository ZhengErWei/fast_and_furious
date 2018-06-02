import csv
from mrjob.job import MRJob


class MRTip15Plus(MRJob):
    '''
    MapReducer for finding trips that have tip amount greater that 15.
    '''
    def mapper(self, _, line):

        row = next(csv.reader([line]))

        if (len(row) > 0) and (row[0] != "VendorID"):
            
            if (int(row[0]) in (1, 2)) and (str(row[1]) < str(row[2])) and \
               (int(row[3]) >= 1) and (float(row[4]) > 0.0) and \
               (float(row[5]) != 0.0) and (float(row[6]) != 0.0) and \
               (int(row[7]) <= 6) and (str(row[8]) == "N") and \
               (float(row[9]) != 0.0) and (float(row[10]) != 0.0) and \
               (int(row[11]) in (1, 2)) and (float(row[12]) > 0.0) and \
               (float(row[13]) >= 0.0) and (float(row[14]) >= 0.0) and \
               (float(row[15]) > 15.0) and (float(row[16]) >= 0.0) and \
               (float(row[17]) >= 0.0) and (float(row[18]) >= 0.0):
                
                yield row, None


class MRTop100(MRJob):
    '''
    MapReducer for finding top 100 pickup, dropoff, tip amount pair.
    '''
    def mapper(self, _, line):

        row = next(csv.reader([line]))

        if (len(row) > 0) and (row[0] != "VendorID"):
        	
            yield (round(float(row[6]), 3), round(float(row[5]), 3), \
            	   round(float(row[10]), 3), round(float(row[9]), 3), \
            	   round(float(row[15]))), 1

    def reducer_init(self):
        self.top100 = []

    def reducer(self, pickup, counts):
        sum_counts = sum(counts)

        if len(self.top100) < 100:
            self.top100.append((pickup, sum_counts))

        else:
            self.top100.sort(key=lambda x: x[1])
            
            if sum_counts > self.top100[0][1]:
                self.top100[0] = (pickup, sum_counts)

    def reducer_final(self):
        self.top100.sort(key=lambda x: x[1], reverse=True)

        for location in self.top100:
            yield location


class MRDeAnonymization(MRJob):
    '''
    MapReducer for labeling trips of known location.
    '''
    def mapper_init(self):

        self.EPSILON = 0.0000005

        self.investment_banks = {"Citigroup" :   ((40.7200, 40.7216), \
                                                (-74.0119, -74.0099)), \
        						"Goldman Sachs": ((40.7141, 40.7152), \
                                                (-74.0143, -74.0136))}

        self.night_clubs = {"Avenue": (40.74405065, -74.00631335), \
        					"Bembe": (40.7111048, -73.9651604), \
        					"Black Flamingo": (40.7104806, -73.9540968), \
        					"C'mon Everybody": (40.6883159, -73.9568761), \
        					"Cielo": (40.7397342, -74.0070218), \
        					"Drink Lounge": (40.6721649, -73.9576687), \
        					"Electric Room": (40.7423311, -74.0034791), \
        					"FREQ": (40.766805, -73.996215), \
        					"Friends and Lovers": (40.678534, -73.958435), \
        					"Good Room": (40.7268963, -73.9529488), \
        					"Grill On The Hill": (40.8222947, -73.9501033), \
        					"House of Yes": (40.706758, -73.92355685), \
        					"Hudson Terrace": (40.7640706, -73.9975569), \
        					"Le Bain": (40.7409232, -74.008111), \
        					"Littlefield": (40.6785007, -73.9833014), \
        					"Marquee New York": (40.7500697, -74.0027905), \
        					"Minton's Playhouse": (40.8046775, -73.9522994), \
        					"Nublu": (40.722524, -73.9797307), \
        					"Output": (40.7223166, -73.9578179): , \
        					"The Jane Hotel": (40.7382579, -74.0094507), \
        					"The Press Lounge": (40.7645758, -73.99595), \
        					"The Rosemont": (40.7071318, -73.9474399), \
        					"Verboten": (40.7220265, -73.9591725)}

        self.luxury_apts = {"Doral Arrowwood Resort": (41.045, -73.689), \
        					"HNA Palisades Premier": (41.019, -73.922), \
        					"Regency Apartments": (41.616, -73.733), \
        					"Renaissance Westchester": (41.017, -73.718), \
        					"Ritz Carlton Residences": (40.705, -74.017), \
                            "Silver Towers": (40.761, -73.999), \
                            "Strathmore Luxury": (40.775, -73.950), \
                            "Westchester Marriott": (41.060, -73.833)}

    def mapper(self, _, line):
        
        row = next(csv.reader([line]))

        if (len(row) > 0) and (row[0] != "VendorID"):

            pu_lat = float(row[6])
            pu_long = float(row[5])
            do_lat = float(row[10])
            do_long = float(row[9])

            for bank, location in self.investment_banks.items():
                if (((location[0][0] <= pu_lat <= location[0][1]) \
                    and (location[1][0] <= pu_long <= location[1][1])) or \
                ((location[0][0] <= do_lat <= location[0][1]) \
                    and (location[1][0] <= do_long <= location[1][1]))):
                    yield (row, bank), None

            
            for club, location in self.night_clubs.items():
                if ((((location[0] - pu_lat) ** 2) + \
                ((location[1] - pu_long) ** 2)) < self.EPSILON) or \
                ((((location[0] - do_long) ** 2) + \
                ((location[1] - do_lat) ** 2)) < self.EPSILON):
                    yield (row, club), None

            pu_lat = round(pu_lat, 3)
            pu_long = round(pu_long, 3)
            do_lat = round(do_lat, 3)
            do_long = round(do_long, 3)

            for apt, location in self.luxury_apts.items():
                if ((pu_lat == location[0]) and (pu_long == location[1])) \
                 or ((do_lat == location[0]) and (do_long == location[1])):
                    yield (row, apt), None
