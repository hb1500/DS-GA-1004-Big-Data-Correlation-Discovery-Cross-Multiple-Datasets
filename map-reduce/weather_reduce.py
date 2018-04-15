#!/usr/bin/env python  
import sys
import numpy as np


def mean(list):
	return sum(list)/len(list)





cur_key = None
count = 0
for line in sys.stdin:
	line.strip()
	key, value = line.split("\t",1)

	attr = value.split(',')

	Spd = int(float(attr[0]))
	Visb = int(float(attr[1]))
	Temp = int(float(attr[2]))
	if Temp == 999:
		continue
	Prcp = int(float(attr[3]))
	if Prcp == 9999:
		continue

	count += 1

	if count == 1:
		cur_key = key
		Spd_list = []
		Visb_list = []
		Temp_list = []
		Prcp_list = []
		Spd_list.append(Spd)
		Visb_list.append(Visb)
		Temp_list.append(Temp)
		Prcp_list.append(Prcp)

	else:
		if key != cur_key:

			min_Spd,mean_Spd,max_Spd = min(Spd_list),mean(Spd_list),max(Spd_list)  
			min_Visb,mean_Visb,max_Visb  = min(Visb_list),mean(Visb_list),max(Visb_list)   
			min_Temp,mean_Temp,max_Temp = min(Temp_list),mean(Temp_list),max(Temp_list)  
			min_Prcp,mean_Prcp,max_Prcp = min(Prcp_list),mean(Prcp_list),max(Prcp_list)   
	
			print("{0}\t{1:.2f},{2:.2f},{3:.2f},{4:.2f},{5:.2f},{6:.2f},{7:.2f},{8:.2f},{9:.2f},{10:.2f},{11:.2f},{12:.2f}".format(key,min_Spd,mean_Spd,max_Spd,min_Visb,mean_Visb,max_Visb,min_Temp,mean_Temp,max_Temp,min_Prcp,mean_Prcp,max_Prcp))

			cur_key = key

			Spd_list = []
			Visb_list = []
			Temp_list = []
			Prcp_list = []
			Spd_list.append(Spd)
			Visb_list.append(Visb)
			Temp_list.append(Temp)
			Prcp_list.append(Prcp)
		else:
			Spd_list.append(Spd)
			Visb_list.append(Visb)
			Temp_list.append(Temp)
			Prcp_list.append(Prcp)



	
	
