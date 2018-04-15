#!/usr/bin/env python
import sys
import csv
import io
import string
#from geopy.geocoders import Nominatim
#geolocator = Nominatim()
### property_price 
# "3046680044",3,4668,44,,"FRANCIS, BOYSIE A",
# "C0","1","25","100","G","2",4.8700000000000000e+05,1.2171000000000000e+04,2.0025000000000000e+04,1.3800000000000000e+03,1.3800000000000000e+03,
# "1017","1055 WILLMOHR STREET","11212",,"20","67"
# ,,,,,,"FINAL","2009/10","AC-TR"


for line in sys.stdin:
	line = line.strip()
	Pre_string=io.StringIO(line)
	#entry = csv.reader(line)
#	entry = line.split(',')
	lines=csv.reader(Pre_string)
	for entry in lines:
		if entry[0] == 'BBLE' and len(entry) == 31:
			continue
		else: 
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
			STORIES = entry[11] # "number of stories in building" 57430 missing values  
			FULLVAL = entry[12] # "market value"
			AVLAND = entry[13] # "actual land value"
			AVTOT = entry[14] # "actual total value"
			EXLAND = entry[15] # "actual exempt land value"
			EXTOT = entry[16] # "actual exemtp land total" 679 missing values  
	#		EXCD1 = entry[17] # "exemption code 1"  430595 missing values ---------remove
			STADDR = entry[18] # "street address" remove 430595 missing values  
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
			print('{0},{1},{2},{3},{4},{5},{6},{7}\t{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23}'.format(YEAR,-1,-1,-1,0,ZIP,-1,1,BBLE,B,BLOCK,LOT,BLDGCL,TAXCLASS,LTFRONT,LTDEPTH,STORIES,FULLVAL,AVLAND,AVTOT,EXLAND,EXTOT,BLDFRONT,BLDDEPTH))



