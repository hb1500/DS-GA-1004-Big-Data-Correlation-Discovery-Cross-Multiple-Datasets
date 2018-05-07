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

NYPD=spark.read.format('csv').options(header='true',inferschema='true').load("/user/jw4937/Big_Data_proj/NYPD/Processed_merged.csv")


NYPD.createOrReplaceTempView("NYPD")

spark.sql("SELECT Processed_start_time, \
	Count (*) as total_count \
	From NYPD \
	Group by Processed_start_time\
	").write.csv("NYPD_groupby_time.csv", header=True)
