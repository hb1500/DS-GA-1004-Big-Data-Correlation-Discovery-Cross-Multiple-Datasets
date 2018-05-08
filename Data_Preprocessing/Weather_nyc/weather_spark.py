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
conf = SparkConf().setAppName("weather")
sc = SparkContext(conf=conf)

def extractor(line):
	Date = line[0]
	Time = line[1]
	Spd = line[2].strip()
	if Spd == '999.9' or Spd == '0':
		Spd = 999999
	Visb = line[3].strip()
	if Visb.strip() ==  '' or int(Visb) == 0:
		Visb = 999999
		# Visb = 14206 ## average from all visb
	Temp = line[4].strip()
	if Temp == '999.9':
		Temp = 999999
	Prcp = line[5].strip()
	if Prcp == '999.9':
		Prcp = 999999
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
	Date = str(Year) + str(Month) + str(Day) + str(hr)
	Year = str(Year)


	return (str(Date),str(Year),str(Spd),str(Visb),str(Temp),str(Prcp))

## inputfile = sc.textFile('/user/zg758/weather-2011-2017.csv')    ## inputfile.take(1)
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

schemaString = "YearDate Year Spd Visb Temp Prcp"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(output, schema)
schema_input.createOrReplaceTempView("output")
# key = spark.sql("SELECT key FROM output")
## key.show(10)


pair=spark.sql("SELECT YearDate, AVG(Year) as Year, AVG(CASE WHEN Spd <> 999999 THEN Spd ELSE NULL END) as Spd, \
	AVG(CASE WHEN Visb <> 999999 THEN Visb ELSE NULL END) as Visb, \
	AVG(CASE WHEN Temp <> 999999 THEN Temp ELSE NULL END) as Temp, \
	AVG(CASE WHEN Prcp <> 999999 THEN Prcp ELSE NULL END) as Prec \
	FROM output GROUP BY YearDate ORDER BY SUBSTRING(YearDate, 1, 4), SUBSTRING(YearDate, 5, 6),\
	SUBSTRING(YearDate, 7, 8),SUBSTRING(YearDate, 9, 10)")

pair.write.save("weather_time.out",format="csv",header = True)


# pair.select(functions.format_string('%s\t%.2f, %.2f, %.2f, %.2f',pair.key, \
# 	pair.mean_Spd, pair.mean_Visb, pair.mean_Prec, pair.mean_Temp)).write.save("weather.out",format="csv")	



