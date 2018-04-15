import sys 
import csv
import io

for line in sys.stdin:
	line = line.strip()
	Pre_string=io.StringIO(line)
	lines=csv.reader(Pre_string)

	for line in lines:
		if line[0] == 'DATE':
			continue
		else:
			DATE = line[0]                                   
			TIME  = line[1]                                 
			BOROUGH = line[2]                            
			ZIP_CODE = line[3]                           
			LATITUDE = line[4]                           
			LONGITUDE = line[5]                          
			LOCATION = line[6]                         
			PERSONS_INJURED = line[10]              
			PERSONS_KILLED = line[11]               
			PEDESTRIANS_INJURED = line[12]          
			PEDESTRIANS_KILLED = line[13]           
			CYCLIST_INJURED = line[14]              
			CYCLIST_KILLED = line[15]               
			MOTORIST_INJURED = line[16]             
			MOTORIST_KILLED = line[17]              
			CON_VEHICLE1 = line[18]      
			CON_VEHICLE2 = line[19]                                 
			VEHICLE_CODE1 = line[24]                 
			VEHICLE_CODE2 = line[25]     

		Year = DATE[6:10]
		Month = DATE[0:2]
		Day = DATE[3:5]

		hr = 0
		if len(TIME) == 4:
			hr = "0"+TIME[0]
		elif len(TIME) == 5:
			hr = TIME[0:2]

		print("{0},{1},{2},{3},{4},{5},{6},{7}\t{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}".format(Year,Month,Day,hr,3,ZIP_CODE,-1,1,BOROUGH,LATITUDE,LONGITUDE,LOCATION,PERSONS_INJURED,PERSONS_KILLED,PEDESTRIANS_INJURED,PEDESTRIANS_KILLED,CYCLIST_INJURED,CYCLIST_KILLED,MOTORIST_INJURED,MOTORIST_KILLED,CON_VEHICLE1,CON_VEHICLE2,VEHICLE_CODE1,VEHICLE_CODE2))








