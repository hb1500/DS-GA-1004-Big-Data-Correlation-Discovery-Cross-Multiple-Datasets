#!/usr/bin/env python  
import sys 
import csv
import io

for line in sys.stdin:
	line = line.strip()
	Pre_string=io.StringIO(line)
	lines=csv.reader(Pre_string)
					
	for line in lines:
		if line[0] == 'Date':
			continue
		else:

			Date = line[0]
			Time = line[1]
			Spd = line[2]
			Visb = line[3]
			if Visb.strip() ==  '':
				Visb = 9999

			Temp = line[4]
			Prcp = line[5]
			# SD = line[6]
			# SDW = line[7]
			# SA = line[8]
			
			Year = Date[0:4]
			Month = Date[4:6]
			Day = Date[6:8]
			
			hr = 0
			if len(Time) == 1 or len(Time) == 2:
				hr = '00'	
			elif len(Time) ==  3:
				hr = '0'+ Time[0]
			elif len(Time) == 4:
				hr = Time[0:2]



			print("{0},{1},{2},{3},{4},{5},{6},{7}\t{8},{9},{10},{11}".format(Year,Month,Day,hr,3,-1,-1,1,Spd,Visb,Temp,Prcp))


