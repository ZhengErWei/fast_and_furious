# Purpose: use this file to clean the mapreduce result 
#           on location or weather on sample_trip.csv
# Command: python3 clean_output.py inputfile outputfile


import csv
import sys

def clean_raw_sample_data(ipfile, opfile):

	with open(ipfile, 'r') as f:

		rb = csv.reader(f)
		for row in rb:
			cond = row[0].strip('["')
			visi, val = row[1].strip(' "').split('"]\t')
			with open(opfile, 'a') as f:

				wb = csv.writer(f, delimiter=',')
				wb.writerow([cond, visi, val])

	return





if __name__ == '__main__':
 	Input, Output = sys.argv[1:]
	clean_raw_sample_data(Input, Output)
