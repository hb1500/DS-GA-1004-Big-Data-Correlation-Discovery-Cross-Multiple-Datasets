#!/usr/bin/python
import numpy as np
from csv import reader
import pickle
from pyspark.sql import SparkSession
from pyspark import SparkContext
from shapely.geometry import Polygon
from shapely.geometry import Point
from pyspark.sql import Row
import glob
sc = SparkContext("local")
spark = SparkSession.builder.appName("Python df").config("some-config", "some-value").getOrCreate()

def mapper(entry):
	
	trip_duration=entry[1]
	start_time=entry[2]
	stop_time=entry[3]
	start_station_id=entry[4]
	start_station_name=entry[5]
	start_station_latitude=entry[6]
	start_station_longitude=entry[7]
	end_station_id=entry[8]
	end_station_name=entry[9]
	end_station_latitude=entry[10]
	end_station_longitude=entry[11]
	bike_id=entry[12]
	user_type=entry[13]
	birth_year=entry[14]
	gender=entry[15]
	start_zipcode=entry[16]
	end_zipcode=entry[17]
	try:
		if (len(start_time.split("-")[0])==4):
			#2016-10-01
			start_Year=start_time.split("-")[0]
			start_Month=start_time.split("-")[1]
			start_Day=start_time.split("-")[2][0:2]
			start_Hour=start_time.split("-")[2].split(":")[0][-2:]

			stop_Year=stop_time.split("-")[0]
			stop_Month=stop_time.split("-")[1]
			stop_Day=stop_time.split("-")[2][0:2]
			stop_Hour=stop_time.split("-")[2].split(":")[0][-2:]

		else:

			start_Year=start_time.split("/")[2][0:4]
			start_Month=start_time.split("/")[0]
			start_Day=start_time.split("/")[1]
			stary_Hour=start_time.split("/")[2][5:7]

			stop_Year=stop_time.split("/")[2][0:4]
			stop_Month=stop_time.split("/")[0]
			stop_Day=stop_time.split("/")[1]
			stop_Hour=stop_time.split("/")[2][5:7]

		if (len(start_Month)==1):
			start_Month="0"+start_Month
		
		if (len(start_Day)==1):
			start_Day="0"+start_Day
		
		if (len(stop_Month)==1):
			end_Month="0"+end_Month

		if (len(stop_Day)==1):
			end_Day="0"+end_Day

		if (start_zipcode==""):
			start_zipcode="99999"

		if (end_zipcode==""):
			end_zipcode="99999"
		
		Processed_start_time=start_Year+start_Month+start_Day+start_Hour

		Processed_stop_time=stop_Year+stop_Month+stop_Day+stop_Hour
	
	except:

		Processed_start_time="0"
		Processed_stop_time="0"
	

	return [Processed_start_time, start_Year, Processed_stop_time, stop_Year, start_station_id, end_station_id, user_type, birth_year,gender, start_zipcode, end_zipcode] 


Citibike_df = sc.textFile("/user/jw4937/Big_Data_proj/Citibike/Prepocessed_merged.csv")
Citibike_df= Citibike_df.mapPartitions(lambda x: reader(x))

header = Citibike_df.first()
Citibike_df = Citibike_df.filter(lambda x: x!=header)
Citibike_df = Citibike_df.map(lambda entry: mapper(entry))
Citibike_df = Citibike_df.filter(lambda x: x[0]!="0")
Citibike_df = Citibike_df.map(lambda p: Row(Processed_start_time=p[0], start_Year=p[1], Processed_stop_time=p[2], stop_Year=p[3], start_station_id=p[4], end_station_id=p[5], user_type=p[6], birth_year=p[7], gender=p[8], start_zipcode=p[9], end_zipcode=p[10]))
Citibike_df = spark.createDataFrame(Citibike_df)

Citibike_df.select("*").write.save("Processed_merged_Citibike.csv", format="csv", header="true")

