import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession

conf = SparkConf().setAppName("taxi")
sc = SparkContext(conf=conf)
inputfile = sc.textFile(sys.argv[1], 1)  
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)

inputfile = inputfile.mapPartitions(lambda x: reader(x))
inputfile = inputfile.filter(lambda x: x != [])
# the input is 14/11-16/06
#if len(header)!=17:
def extractor(line):
	
	
	pickDate = line[1]
	dropDate = line[2]
	num_pass = line[3]
	trip_dis = line[4]
			#rate_ID = line[7]
	if len(header)==17:
		fare_amount = line[10]
		tip_amount = line[13]
	else:
		fare_amount = line[12]
		tip_amount = line[15]
	 		#total_amount = line[18]
	pickyear = pickDate[0:4]
	pickmon = pickDate[5:7]
	pickday = pickDate[8:10]
	pickhr = pickDate[11:13]
	dropyear = dropDate[0:4]
	dropmon = dropDate[5:7]
	dropday = dropDate[8:10]
	drophr = dropDate[11:13]
	if pickday == dropday:
		trip_time = (float(drophr)-float(pickhr))*60 + (float(dropDate[14:16])-float(pickDate[14:16]))
	else:
		trip_time = (24 - float(pickhr)+float(drophr))* 60 + (60-float(pickDate[14:16]) + float(dropDate[14:16] ))
	key = str(pickyear) + ',' + str(pickmon) + ',' + str(pickday) + ',' + str(pickhr) + ',' + str(3) + ',' + str(-1) + ',' + str(-1)+ ',' + str(1)
	return (key,str(trip_time),str(num_pass),str(trip_dis),str(fare_amount),str(tip_amount))
	#before 14/11
# if len(header)==18:
# 	def extractor(line):
# 		pickDate = line[1]
# 		dropDate = line[2]
# 		num_pass = line[3]
# 		trip_dis = line[4]
# 		#rate_ID = line[7]
# 		fare_amount = line[12]
		
# 		tip_amount = line[15]
#  		#total_amount = line[17]
#  		pickyear = pickDate[:4]
#  		pickmon = pickDate[5:7]
#  		pickday = pickDate[8:10]
#  		pickhr = pickDate[11:13]
#  		dropyear = dropDate[:4]
#  		dropmon = dropDate[5:7]
#  		dropday = dropDate[8:10]
#  		drophr = dropDate[11:13]
#  		if pickday == dropday:
#  			trip_time = (float(drophr)-float(pickhr))*60 + (float(dropDate[14:16]-float(pickDate[14:16])))
#  		else:
#  			trip_time = (24 - float(pickhr)+float(drophr))* 60 + (60-float(pickDate[14:16]) + float(dropDate[14:16] ))
#  		key = str(pickear) + ',' + str(pickmon) + ',' + str(pickday) + ',' + str(pickhr) + ',' + str(3) + ',' + str(-1) + ',' + str(-1)+ ',' + str(1)
#  		return (key,str(num_pass),str(trip_dis),str(rate_ID),str(fare_amount),str(tip_amount),str(total_amount))
# #after 16/06
# else:
# 	def extractor(line):
# 		pickDate = line[1]
# 		dropDate = line[2]
# 		num_pass = line[3]
# 		trip_dis = line[4]
# 		#rate_ID = line[5]
# 		fare_amount = line[10]
		
# 		tip_amount = line[13]
# 		#total_amount = line[16]
# 		pickyear = pickDate[0:4]
# 		pickmon = pickDate[5:7]
# 		pickday = pickDate[8:10]
# 		pickhr = pickDate[11:13]
# 		dropyear = dropDate[0:4]
# 		dropmon = dropDate[5:7]
# 		dropday = dropDate[8:10]
# 		drophr = dropDate[11:13]
# 		if pickday == dropday:
# 			trip_time = (float(drophr)-float(pickhr))*60 + (float(dropDate[14:16])-float(pickDate[14:16]))
# 		else:
# 			trip_time = (24 - float(pickhr)+float(drophr))* 60 + (60-float(pickDate[14:16]) + float(dropDate[14:16] ))
# 		key = str(pickyear) + ',' + str(pickmon) + ',' + str(pickday) + ',' + str(pickhr) + ',' + str(3) + ',' + str(-1) + ',' + str(-1)+ ',' + str(1)
# 		return (key,str(trip_time),str(num_pass),str(trip_dis),str(fare_amount),str(tip_amount))
output = inputfile.map(extractor)

spark = SparkSession \
.builder \
.appName("taxi") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()



schemaString = "key trip_time num_pass trip_dis fare_amount tip_amount"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
schema_input = spark.createDataFrame(output, schema)
schema_input.createOrReplaceTempView("output")
key = spark.sql("SELECT key FROM output")
## key.show(10)
pair=spark.sql("SELECT key,SUM(trip_time) as sum_trip_time, AVG(trip_time) as mean_trip_time, SUM(num_pass) as sum_pass,AVG(num_pass) as mean_pass, SUM(trip_dis) as sum_dis,AVG(trip_dis) as mean_dic,  SUM(fare_amount) as sum_fare,AVG(fare_amount) as mean_fare,  SUM(tip_amount) as sum_tip,AVG(tip_amount) as mean_tip FROM output GROUP BY key ORDER BY substring(key,1,4),substring(key,6,7),substring(key, 9,10),substring(key, 12,13)")


pair.write.save("taxi.out",format="csv",header = True)




