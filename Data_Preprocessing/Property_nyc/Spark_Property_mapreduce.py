import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark import SparkContext
from csv import reader
import string
import io 
from pyspark.sql import functions
from pyspark.sql.session import SparkSession



conf = SparkConf().setAppName("property")
sc = SparkContext(conf=conf)

def extractor(entry):
	BBLE = entry[0] # unqiue code ID
	B = entry[1] # "borough valid values 1-5"
	BLOCK = entry[2] # BLOCK
	LOT = entry[3] # losts 
#		EASEMENT = entry[4] # basements 1055897 missing values ------remove
#		OWNER = entry[5] # remove 30622 
	BLDGCL = entry[6] # "building class"
	TAXCLASS = entry[7] # taxclass catergritcal
	LTFRONT = entry[8] # "lot width"
	LTDEPTH = entry[9] # "lot depth"
#		EXT = entry[10] # "extension indicator" 706025 missing values  ---------remove
#		STORIES = entry[11] # "number of stories in building" 57430 missing values  
	FULLVAL = entry[12] # "market value"
	AVLAND = entry[13] # "actual land value"
	AVTOT = entry[14] # "actual total value"
	EXLAND = entry[15] # "actual exempt land value"
	EXTOT = entry[16] # "actual exemtp land total" 679 missing values  
#		EXCD1 = entry[17] # "exemption code 1"  430595 missing values ---------remove
#		STADDR = entry[18] # "street address" remove 430595 missing values  
#		ZIP = entry[19] # zip 30494 missing values 
	if entry[19] == '':
#			if STADDR != '':
#				try:
#					location = geolocator.geocode(STADDR + ' NYC')
#					ZIP_ = location.address.split(',')[-2].strip()
#				except:
#					ZIP = '99999'
#			else:
#				ZIP = '99999'
		ZIP = "99999"
	else:
		ZIP = entry[19]
#		EXMPTCL = entry[20] # "exempt class" 1044892 missing values -----------remove
	BLDFRONT = entry[21] # "building width"
	BLDDEPTH = entry[22] # "building depth" 
#		AVLAND2 = entry[23] # "transitional land value" 785889 missing values ---------remove 
#		AVTOT2 = entry[24] # "transitional total value" 785889 missing values ---------remove
#		EXLAND2 = entry[25] # "transitional exempt land value" 977889 missing values ---------remove 
#		EXTOT2 = entry[26] # "transitional exempt land total" 939384 missing values ---------remove 
#		EXCD2= entry[27] # "exemption code 2"  970264 MISSING VALUES ----------remove 
#		PERIOD= entry[28] # "assessment period when file was created"
	YEAR = entry[29][0:4] # "assessment year"
#		VALTYPE = entry[30] # AC-TR single value -------remove
	# YYYY,MM,DD,HH,0/1/2/3, zip, neighborhood, 1 \t contributes 
#	value = str(BBLE), str(B), str(BLOCK), str(LOT), str(BLDGCL), str(TAXCLASS), str(LTFRONT), str(LTDEPTH), str(FULLVAL), str(AVLAND), str(AVTOT), str(EXLAND), str(EXTOT), str(BLDFRONT), str(BLDDEPTH)
	return (str(YEAR),str(ZIP),str(BBLE), str(B), str(BLOCK), str(LOT), str(BLDGCL), str(TAXCLASS), str(LTFRONT), str(LTDEPTH), str(FULLVAL), str(AVLAND), str(AVTOT), str(EXLAND), str(EXTOT), str(BLDFRONT), str(BLDDEPTH))

#inputfile = sc.textFile('/user/hb1500/property_data/avroll_*.csv')
inputfile = sc.textFile(sys.argv[1], 1)
header = inputfile.first()
inputfile = inputfile.filter(lambda x: x!=header)
inputfile = inputfile.mapPartitions(lambda x: reader(x))
output = inputfile.map(extractor)
# save spark RDD output:
output.saveAsTextFile("property_RDD_result.out")

spark = SparkSession \
.builder \
.appName("weather") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

# adding column names for dataframe:
schemaString = "Year ZIP BBLE B BLOCK LOT BLDGCL TAXCLASS LTFRONT LTDEPTH FULLVAL AVLAND AVTOT EXLAND EXTOT BLDFRONT BLDDEPTH"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

schema_input = spark.createDataFrame(output, schema)
# save spark sql dataframe output: 

schema_input.createOrReplaceTempView("output")
result = spark.sql("SELECT * FROM output")
result.createOrReplaceTempView("result")
result.write.save("property_dataframe_output.out",format="csv")



