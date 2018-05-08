import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession

## sc.textFile('/user/zg758/1004/weather-2011-2017.csv')
conf = SparkConf().setAppName("collision")
sc = SparkContext(conf=conf)


def extractor(line):
	DATE = line[0]                                   
	TIME  = line[1]                                 
	# BOROUGH = line[2]                            
	ZIP_CODE = line[3]  
	if ZIP_CODE.strip() == '':  ### pasa those rows whose zipcode is nan 
		ZIP_CODE = 99999             
	# LATITUDE = line[4]                           
	# LONGITUDE = line[5]                          
	# LOCATION = line[6]                         
	PERSONS_INJURED = int(line[10])              
	PERSONS_KILLED =  int(line[11])               
	PEDESTRIANS_INJURED = int(line[12])           
	PEDESTRIANS_KILLED = int(line[13])        
	CYCLIST_INJURED = int(line[14])         
	CYCLIST_KILLED = int(line[15])            
	MOTORIST_INJURED = int(line[16])             
	MOTORIST_KILLED = int(line[17])           
	# CON_VEHICLE1 = line[18]      
	# CON_VEHICLE2 = line[19]                                 
	# VEHICLE_CODE1 = line[24]                 
	# VEHICLE_CODE2 = line[25]    
	Year = DATE[6:10]
	Month = DATE[0:2]
	Day = DATE[3:5]
	hr = 0
	if len(TIME) == 4:
		hr = "0"+TIME[0]
	elif len(TIME) == 5:
		hr = TIME[0:2]
	Date = str(Year)  + str(Month) + str(Day) + str(hr)
	Year = str(Year) 
	ZIP = str(ZIP_CODE)
	return (Date,Year,ZIP,str(PERSONS_INJURED+PERSONS_KILLED),str(PEDESTRIANS_INJURED+PEDESTRIANS_KILLED),
		str(CYCLIST_INJURED+CYCLIST_KILLED),str(MOTORIST_INJURED+MOTORIST_KILLED))

## inputfile = sc.textFile('/user/zg758/NYPD_Motor_Vehicle_Collisions.csv')    ## inputfile.take(1)
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)
inputfile = inputfile.mapPartitions(lambda x: reader(x))
output = inputfile.map(extractor)



## SQL
spark = SparkSession \
.builder \
.appName("weather") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

schemaString = "YearDate Year ZIP PERSON PEDESTRIAN CYCLIST MOTORIST"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(output, schema)
schema_input.createOrReplaceTempView("output")



pair=spark.sql("SELECT YearDate, AVG(int(Year)) as Year, SUM(PERSON) as Person, \
	SUM(PEDESTRIAN) as Pedestrian, SUM(CYCLIST) as Cyclist, SUM(MOTORIST) as Motorist \
	FROM output GROUP BY YearDate ORDER BY SUBSTRING(YearDate, 1, 4), SUBSTRING(YearDate, 5, 6),\
	SUBSTRING(YearDate, 7, 8),SUBSTRING(YearDate, 9, 10)")

pair.write.save("collision_time.out",format="csv",header = True)


pair=spark.sql("SELECT Zip, SUM(PERSON) as Person, \
	SUM(PEDESTRIAN) as Pedestrian, SUM(CYCLIST) as Cyclist, SUM(MOTORIST) as Motorist \
	FROM output GROUP BY Zip ORDER BY Zip")


pair.write.save("collision_zip.out",format="csv",header = True)





# pair=spark.sql("SELECT ZIP,  SUM(PERSONS_INJURED) as TOTAL_PER_I, SUM(PERSONS_KILLED) as TOTAL_PER_K, \
# 	SUM(PEDESTRIANS_INJURED) as TOTAL_PED_I, SUM(PEDESTRIANS_KILLED) as TOTAL_PED_K, SUM(CYCLIST_INJURED) as \
# 	TOTAL_CYC_I, SUM(CYCLIST_KILLED) as TOTAL_CYC_K, SUM(MOTORIST_INJURED) as TOTAL_MOT_I, SUM(MOTORIST_KILLED) as \
# 	TOTAL_MOT_K FROM output GROUP BY ZIP")

# pair.select(functions.format_string('%s\t%.2f, %.2f, %.2f, %.2f,%.2f, %.2f, %.2f, %.2f',pair.key, \
# 	pair.TOTAL_PER_I,pair.TOTAL_PER_K, pair.TOTAL_PED_I, pair.TOTAL_PED_K, pair.TOTAL_CYC_I, \
# 	pair.TOTAL_CYC_K, pair.TOTAL_MOT_I,pair.TOTAL_MOT_K)).write.save("collision.out",format="csv")	






