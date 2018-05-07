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

Citibike=spark.read.format('csv').options(header='true',inferschema='true').load("/user/jw4937/Big_Data_proj/Citibike/Processed_merged.csv")


Citibike.createOrReplaceTempView("Citibike")

spark.sql("SELECT Processed_start_time, start_zipcode, SUM(CASE WHEN gender = 0 then 1 else 0 end) as Man,\
	SUM(CASE WHEN gender = 1 then 1 else 0 end) as Women, SUM(CASE WHEN gender = 2 then 1 else 0 end) as NotSpecified,\
	SUM(CASE WHEN user_type='Subscriber' then 1 else 0 end) as Subscriber,\
	SUM(CASE WHEN user_type='Subscriber' then 0 else 1 end) as Not_Subscriber,\
	Count (*) as total_count \
	From Citibike \
	Group by Processed_start_time, start_zipcode\
	").write.csv("Citibike_group_by_zip_and_time.csv", header=True)

spark.sql("SELECT Processed_start_time, SUM(CASE WHEN gender = 0 then 1 else 0 end) as Man,\
	SUM(CASE WHEN gender = 1 then 1 else 0 end) as Women, SUM(CASE WHEN gender = 2 then 1 else 0 end) as NotSpecified,\
	SUM(CASE WHEN user_type='Subscriber' then 1 else 0 end) as Subscriber,\
	SUM(CASE WHEN user_type='Subscriber' then 0 else 1 end) as Not_Subscriber,\
	Count (*) as total_count \
	From Citibike \
	Group by Processed_start_time \
	").write.csv("Citibike_group_by_time.csv", header=True)



