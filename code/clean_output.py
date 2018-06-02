# python3 clean_weather_output.py inputfile outputfile

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

def clean_raw_ind_data(self, row):

	# ind_1 = row[0].strip("'[")
	# ind_2 = row[1].strip(" '")
	# ind_3 = row[2].strip(" '")
	# ind_4 = row[3].strip(" '")
	# ind_5 = row[4].strip(" '")
	# ind_6, y = row[5].strip(" '").split(']\t')

	ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y = row[1:]

	rv = [ind_1, ind_2, ind_3, ind_4, ind_5, ind_6, y]
	rv = [float(a) for a in rv]

	return rv



# if __name__ == '__main__':
# 	Input, Output = sys.argv[1:]
# 	# clean_raw_sample_data(Input, Output)