#!/usr/bin/python
import numpy as np
from csv import reader
import pickle
from pyspark.sql import SparkSession
from pyspark import SparkContext
from shapely.geometry import Polygon
from shapely.geometry import Point
from pyspark.sql import Row

sc = SparkContext("local")
spark = SparkSession.builder.appName("Python df").config("some-config", "some-value").getOrCreate()

def mapper(entry):
	
	Unique_Key=entry[0]
	Created_Date=entry[1]
	Closed_Date=entry[2]
	Agency=entry[3]
	Agency_Name=entry[4]
	Complaint_Type=entry[5]
	Descriptor=entry[6]
	Location_Type=entry[7]
	Incident_Zip=entry[8]
	Incident_Address=entry[9]
	Street_Name=entry[10]
	Cross_Street_1=entry[11]
	Cross_Street_2=entry[12]
	Intersection_Street_1=entry[13]
	Intersection_Street_2=entry[14]
	Address_Type=entry[15]
	City=entry[16]
	Landmark=entry[17]
	Facility_Type=entry[18]
	Status=entry[19]
	Due_Date=entry[20]
	Resolution_Description=entry[21]
	Resolution_Action_Updated_Date=entry[22]
	Community_Board=entry[23]
	Borough=entry[24]
	X_Coordinate_State_Plane=entry[25]
	Y_Coordinate_State_Plane=entry[26]
	Park_Facility_Name=entry[27]
	Park_Borough=entry[28]
	School_Name=entry[29]
	School_Number=entry[30]
	School_Region=entry[31]
	School_Code=entry[32]
	School_Phone_Number=entry[33]
	School_Address=entry[34]
	School_City=entry[35]
	School_State=entry[36]
	School_Zip=entry[37]
	School_Not_Found=entry[38]
	School_or_Citywide_Complaint=entry[39]
	Vehicle_Type=entry[40]
	Taxi_Company_Borough=entry[41]
	Taxi_Pick_Up_Location=entry[42]
	Bridge_Highway_Name=entry[43]
	Bridge_Highway_Direction=entry[44]
	Road_Ramp=entry[45]
	Bridge_Highway_Segment=entry[46]
	Garage_Lot_Name=entry[47]
	Ferry_Direction=entry[48]
	Ferry_Terminal_Name=entry[49]
	Latitude=entry[50]
	Longitude=entry[51]
	Location=entry[52]

	Created_Year=Created_Date[6:10]
	Created_Day=Created_Date[3:5]
	Created_Month=Created_Date[0:2]
	Created_Hour=Created_Date[11:13]
	AM_PM=Created_Date[-2:]
	
	if (AM_PM=="PM"):
		Created_Hour=str(int(Created_Hour)+12)

	if (Incident_Zip==""):
		Incident_Zip="99999"
	
	Processed_start_time=Created_Year+Created_Month+Created_Day+Created_Hour

	return [Processed_start_time, Agency, Complaint_Type, Incident_Zip, Created_Year]


df_311_service = sc.textFile("/user/jw4937/Big_Data_proj/311_Service/merged.csv")
df_311_service = df_311_service.mapPartitions(lambda x: reader(x))

header = df_311_service.first()
df_311_service = df_311_service.filter(lambda x: x!=header)
df_311_service = df_311_service.map(lambda entry: mapper(entry))
df_311_service = df_311_service.map(lambda p: Row(Processed_start_time=p[0], Agency=p[1], Complaint_Type=p[2], Incident_Zip=p[3], Created_Year=p[4]))
df_311_service=spark.createDataFrame(df_311_service)

df_311_service.select("*").write.save("/user/jw4937/Big_Data_proj/Citibike/Processed_merged.csv", format="csv", header="true")

