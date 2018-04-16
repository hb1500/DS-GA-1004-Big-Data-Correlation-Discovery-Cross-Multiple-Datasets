#!/usr/bin/env python
#Reduce function for computing matrix multiply A*B

#Input arguments:
#variable n should be set to the inner dimension of the matrix product (i.e., the number of columns of A/rows of B)

import sys

#number of columns of A/rows of B


#Create data structures to hold the current row/column values (if needed; your code goes here)

currentkey = None
counter=0

Unique_Key=set()
Agency=set()
Complaint_Type=set()
Location_Type=set()
Facility_Type=set()
Status=set()

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

        #Remove leading and trailing whitespace
        line = line.strip()

        #Get key/value 
        key, value = line.split('\t',1)

        #Parse key/value input (your code goes here)
        entry=value.split(",")
        #If we are still on the same key...
        if key==currentkey:

                #Process key/value pair (your code goes here)
                # if (value!="open"):
                #         counter=counter+1
                # else:
                counter=counter+1

                Unique_Key.add(entry[0])
                Agency.add(entry[1])
                Complaint_Type.add(entry[2])
                Location_Type.add(entry[3])
                Facility_Type.add(entry[4])
                Status.add(entry[5])
                #print ("a")
                counter=counter+1
        #Otherwise, if this is a new key...
        else:
                #If this is a new key and not the first key we've seen
                if currentkey:
                    print ('{0:s}\t{1:d},{2:d},{3:d},{4:d},{5:d},{6:d},{7:d}'.format(currentkey, len(Unique_Key), len(Agency), len(Complaint_Type), len(Location_Type), len(Facility_Type), len(Status), counter))
        
                    Unique_Key=set()
                    Agency=set()
                    Complaint_Type=set()
                    Location_Type=set()
                    Facility_Type=set()
                    Status=set()
                    
                    Unique_Key.add(entry[0])
                    Agency.add(entry[1])
                    Complaint_Type.add(entry[2])
                    Location_Type.add(entry[3])
                    Facility_Type.add(entry[4])
                    Status.add(entry[5])
                    counter=1
                else:
                        Unique_Key.add(entry[0])
                        Agency.add(entry[1])
                        Complaint_Type.add(entry[2])
                        Location_Type.add(entry[3])
                        Facility_Type.add(entry[4])
                        Status.add(entry[5])
                        counter=1
                currentkey=key
                
                
                
                        
                        #compute/output result to STDOUT (your code goes here)
print ('{0:s}\t{1:d},{2:d},{3:d},{4:d},{5:d},{6:d},{7:d}'.format(currentkey, len(Unique_Key), len(Agency), len(Complaint_Type), len(Location_Type), len(Facility_Type), len(Status), counter))
                                       


