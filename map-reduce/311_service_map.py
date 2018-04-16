#!/usr/bin/env python
# map function for matrix multiply
#Input file assumed to have lines of the form "A,i,j,x", where i is the row index, j is the column index, and x is the value in row i, column j of A. Entries of A are followed by lines of the form "B,i,j,x" for the matrix B. 
#It is assumed that the matrix dimensions are such that the product A*B exists. 

#Input arguments:
#m should be set to the number of rows in A, p should be set to the number of columns in B.

import sys
import csv
import string
import io
#from geopy.geocoders import Nominatim

#geolocator = Nominatim()

reader=csv.reader(sys.stdin)
#next(reader)

# input comes from STDIN (stream data that goes to the program)
for entry in reader:
        if (entry[0]=="Unique Key"):
                continue
        Unique_Key=entry[0]
        Created_Date=entry[1]
        Closed_Date=entry[2]
        Agency=entry[3]
        Agency_Name=entry[4]
        Complaint_Type=entry[5]
        Descriptor=entry[6]
        Location_Type=entry[7]
        Incident_Zip=entry[8]
        Incident_Address=entry[9]
        Street_Name=entry[10]
        Cross_Street_1=entry[11]
        Cross_Street_2=entry[12]
        Intersection_Street_1=entry[13]
        Intersection_Street_2=entry[14]
        Address_Type=entry[15]
        City=entry[16]
        Landmark=entry[17]
        Facility_Type=entry[18]
        Status=entry[19]
        Due_Date=entry[20]
        Resolution_Description=entry[21]
        Resolution_Action_Updated_Date=entry[22]
        Community_Board=entry[23]
        Borough=entry[24]
        X_Coordinate_State_Plane=entry[25]
        Y_Coordinate_State_Plane=entry[26]
        Park_Facility_Name=entry[27]
        Park_Borough=entry[28]
        School_Name=entry[29]
        School_Number=entry[30]
        School_Region=entry[31]
        School_Code=entry[32]
        School_Phone_Number=entry[33]
        School_Address=entry[34]
        School_City=entry[35]
        School_State=entry[36]
        School_Zip=entry[37]
        School_Not_Found=entry[38]
        School_or_Citywide_Complaint=entry[39]
        Vehicle_Type=entry[40]
        Taxi_Company_Borough=entry[41]
        Taxi_Pick_Up_Location=entry[42]
        Bridge_Highway_Name=entry[43]
        Bridge_Highway_Direction=entry[44]
        Road_Ramp=entry[45]
        Bridge_Highway_Segment=entry[46]
        Garage_Lot_Name=entry[47]
        Ferry_Direction=entry[48]
        Ferry_Terminal_Name=entry[49]
        Latitude=entry[50]
        Longitude=entry[51]
        Location=entry[52]

        Year=Created_Date[6:10]
        Month=Created_Date[0:2]
        Day=Created_Date[3:5]
        Hour=Created_Date[11:13]
        print ('{0:s},{1:s},{2:s},{3:s},{4:s},{5:s},{6:s},{7:s}\t{8:s},{9:s},{10:s},{11:s},{12:s},{13:s}'.format(Year, Month, Day, Hour, "3", Incident_Zip, Borough, "1", Unique_Key, Agency, Complaint_Type, Location_Type, Facility_Type, Status))
        #print (starttime, Zipcode, Neighbourhood, "sa", tripduration)