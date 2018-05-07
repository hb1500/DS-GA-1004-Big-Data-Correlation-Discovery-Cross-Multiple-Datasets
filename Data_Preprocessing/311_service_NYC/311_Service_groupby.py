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

df_311=spark.read.format('csv').options(header='true',inferschema='true').load("/user/jw4937/Big_Data_proj/311_service/Processed_merged.csv")


df_311.createOrReplaceTempView("df_311")

spark.sql("SELECT Processed_start_time, Incident_Zip, \
	Count (*) as total_count \
	From df_311 \
	Group by Processed_start_time, Incident_Zip\
	").write.csv("311_group_by_zip_and_time.csv", header=True)
