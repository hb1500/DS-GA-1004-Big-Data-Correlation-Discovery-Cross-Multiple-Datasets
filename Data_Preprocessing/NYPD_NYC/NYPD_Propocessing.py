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
	
	CMPLNT_NUM=entry[0]
	CMPLNT_FR_DT=entry[1]
	CMPLNT_FR_TM=entry[2]
	CMPLNT_TO_DT=entry[3]
	CMPLNT_TO_TM=entry[4]
	RPT_DT=entry[5]
	KY_CD=entry[6]
	OFNS_DESC=entry[7]
	PD_CD=entry[8]
	PD_DESC=entry[9]
	CRM_ATPT_CPTD_CD=entry[10]
	LAW_CAT_CD=entry[11]
	JURIS_DESC=entry[12]
	BORO_NM=entry[13]
	ADDR_PCT_CD=entry[14]
	LOC_OF_OCCUR_DESC=entry[15]
	PREM_TYP_DESC=entry[16]
	PARKS_NM=entry[17]
	HADEVELOPT=entry[18]
	X_COORD_CD=entry[19]
	Y_COORD_CD=entry[20]
	Latitude=entry[21]
	Longitude=entry[22]
	Lat_Lon=entry[23]
	

	Created_Year=CMPLNT_FR_DT[6:10]
	Created_Day=CMPLNT_FR_DT[3:5]
	Created_Month=CMPLNT_FR_DT[0:2]
	Created_Hour=CMPLNT_FR_TM[0:2]
	
	# if (AM_PM=="PM"):
	# 	Created_Hour=str(int(Created_Hour)+12)

	# if (Incident_Zip==""):
	# 	Incident_Zip="99999"
	
	Processed_start_time=Created_Year+Created_Month+Created_Day+Created_Hour

	return [Processed_start_time, KY_CD, PD_CD, LAW_CAT_CD, ADDR_PCT_CD, CRM_ATPT_CPTD_CD, Created_Year]


NYPD_Complaint = sc.textFile("/user/jw4937/Big_Data_proj/NYPD_Compliant/NYPD_Complaint_Data_Historic.csv")
NYPD_Complaint = NYPD_Complaint.mapPartitions(lambda x: reader(x))

header = NYPD_Complaint.first()
NYPD_Complaint = NYPD_Complaint.filter(lambda x: x!=header)
NYPD_Complaint = NYPD_Complaint.map(lambda entry: mapper(entry))
NYPD_Complaint = NYPD_Complaint.map(lambda p: Row(Processed_start_time=p[0], KY_CD=p[1], PD_CD=p[2], LAW_CAT_CD=p[3], ADDR_PCT_CD=p[4], CRM_ATPT_CPTD_CD=p[5], Created_Year=p[6]))
NYPD_Complaint=spark.createDataFrame(NYPD_Complaint)

NYPD_Complaint.select("*").write.save("Processed_NYPD_Complaint_Data_Historic.csv", format="csv", header='true')

