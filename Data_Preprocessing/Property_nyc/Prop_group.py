from pyspark.sql import SparkSession
from pyspark.sql import functions
import sys
from csv import reader
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

prop = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
# prop = spark.read.format('csv').options(header='true',inferschema='true').load('/user/hb1500/property_preprocessed.csv')
prop.createOrReplaceTempView("prop")
# Year, Zip, Yaer_Zip, BBLE, B,BLOCK,LOT,BLDGCL,TAXCLASS,LTFRONT,LTDEPTH,FULLVAL,AVLAND,AVTOT,EXLAND,EXTOT,BLDFRONT,BLDDEPTH,
### aggregate by Zip
prop1 = spark.sql("SELECT Zip, Year, COUNT(BBLE) AS Prop_Cnt, SUM(IF(B = 1, 1, 0)) AS cntb_1,SUM(IF(B = 2, 1, 0)) AS cntb_2,SUM(IF(B = 3, 1, 0)) AS cntb_3,\
	SUM(IF(B = 4, 1, 0)) AS cntb_4,SUM(IF(B = 5, 1, 0)) AS cntb_5, SUM(IF( TAXCLASS= '1A', 1, 0)) AS TAX_1A,\
	SUM(IF( TAXCLASS= 3, 1, 0)) AS TAX_3, SUM(IF( TAXCLASS= '1C', 1, 0)) AS TAX_1C,SUM(IF( TAXCLASS= '2A', 1, 0)) AS TAX_2A,SUM(IF( TAXCLASS= '1D', 1, 0)) AS TAX_1D,\
	SUM(IF( TAXCLASS= '2B', 1, 0)) AS TAX_2B, SUM(IF( TAXCLASS= 1, 1, 0)) AS TAX_1,SUM(IF( TAXCLASS= '1B', 1, 0)) AS TAX_1B, SUM(IF( TAXCLASS= 4, 1, 0)) AS TAX_4,\
	SUM(IF( TAXCLASS= '2C', 1, 0)) AS TAX_2C, SUM(IF( TAXCLASS= 2, 1, 0)) AS TAX_2,\
	avg(LOT), sum(LOT), max(LOT), min(LOT),\
	avg(BLDGCL), sum(BLDGCL), max(BLDGCL), min(BLDGCL),\
	avg(LTFRONT), sum(LTFRONT), max(LTFRONT), min(LTFRONT),\
	avg(LTDEPTH), sum(LTDEPTH), max(LTDEPTH), min(LTDEPTH),\
	avg(FULLVAL), sum(FULLVAL), max(FULLVAL), min(FULLVAL),\
	avg(AVLAND), sum(AVLAND), max(AVLAND), min(AVLAND),\
	avg(AVTOT), sum(AVTOT), max(AVTOT), min(AVTOT),\
	avg(EXLAND), sum(EXLAND), max(EXLAND), min(EXLAND),\
	avg(EXTOT), sum(EXTOT), max(EXTOT), min(EXTOT),\
	avg(BLDFRONT), sum(BLDFRONT), max(BLDFRONT), min(BLDFRONT),\
	avg(BLDDEPTH), sum(BLDDEPTH), max(BLDDEPTH), min(BLDDEPTH) FROM prop GROUP BY Zip, Year")

prop2 = spark.sql("SELECT Zip, Year, COUNT(BBLE) AS Prop_Cnt, SUM(IF(B = 1, 1, 0)) AS cntb_1,SUM(IF(B = 2, 1, 0)) AS cntb_2,SUM(IF(B = 3, 1, 0)) AS cntb_3,\
	SUM(IF(B = 4, 1, 0)) AS cntb_4,SUM(IF(B = 5, 1, 0)) AS cntb_5, SUM(IF( TAXCLASS= '1A', 1, 0)) AS TAX_1A,\
	SUM(IF( TAXCLASS= 3, 1, 0)) AS TAX_3, SUM(IF( TAXCLASS= '1C', 1, 0)) AS TAX_1C,SUM(IF( TAXCLASS= '2A', 1, 0)) AS TAX_2A,SUM(IF( TAXCLASS= '1D', 1, 0)) AS TAX_1D,\
	SUM(IF( TAXCLASS= '2B', 1, 0)) AS TAX_2B, SUM(IF( TAXCLASS= 1, 1, 0)) AS TAX_1,SUM(IF( TAXCLASS= '1B', 1, 0)) AS TAX_1B, SUM(IF( TAXCLASS= 4, 1, 0)) AS TAX_4,\
	SUM(IF( TAXCLASS= '2C', 1, 0)) AS TAX_2C, SUM(IF( TAXCLASS= 2, 1, 0)) AS TAX_2,\
	avg(LOT), sum(LOT), max(LOT), min(LOT),\
	avg(BLDGCL), sum(BLDGCL), max(BLDGCL), min(BLDGCL),\
	avg(LTFRONT), sum(LTFRONT), max(LTFRONT), min(LTFRONT),\
	avg(LTDEPTH), sum(LTDEPTH), max(LTDEPTH), min(LTDEPTH),\
	avg(FULLVAL), sum(FULLVAL), max(FULLVAL), min(FULLVAL),\
	avg(AVLAND), sum(AVLAND), max(AVLAND), min(AVLAND),\
	avg(AVTOT), sum(AVTOT), max(AVTOT), min(AVTOT),\
	avg(EXLAND), sum(EXLAND), max(EXLAND), min(EXLAND),\
	avg(EXTOT), sum(EXTOT), max(EXTOT), min(EXTOT),\
	avg(BLDFRONT), sum(BLDFRONT), max(BLDFRONT), min(BLDFRONT),\
	avg(BLDDEPTH), sum(BLDDEPTH), max(BLDDEPTH), min(BLDDEPTH) FROM prop GROUP BY Zip")



prop1.createOrReplaceTempView("prop1")
prop1.write.save("prop_by_zip_year.csv",format="csv")

prop2.createOrReplaceTempView("prop2")
prop2.write.save("prop_by_zip.csv",format="csv")