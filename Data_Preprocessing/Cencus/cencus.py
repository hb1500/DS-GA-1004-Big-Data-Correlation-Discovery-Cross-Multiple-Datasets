import pandas as pd
import numpy as np
import pickle
import csv
'''
Census data is not big but contains too much columns, (over 520 columns). Therefore, only this datasets was processed in python rather Pyspark.
'''
census_2016 = pd.read_csv('/Users/hetianbai/Desktop/Census_Data/ACS_16_5YR_DP03_with_ann.csv', na_values = ["(X)",'-','**'])
census_2015 = pd.read_csv('/Users/hetianbai/Desktop/Census_Data/ACS_15_5YR_DP03_with_ann.csv', na_values = ["(X)",'-','**'])
census_2014 = pd.read_csv('/Users/hetianbai/Desktop/Census_Data/ACS_14_5YR_DP03_with_ann.csv', na_values = ["(X)",'-','**'])
census_2013 = pd.read_csv('/Users/hetianbai/Desktop/Census_Data/ACS_13_5YR_DP03_with_ann.csv', na_values = ["(X)",'-','**'])

# unify column names: 
col = census_2016.columns.tolist()
census_2015.columns = col
census_2014.columns = col
census_2013.columns = col
# merge datasets by columns vertically 
merged = pd.concat([census_2016, census_2015, census_2014, census_2013],axis=0)

### remove columns contains too much missing valus 
pct_null = merged.isnull().sum()/len(merged)
missing_features = pct_null[pct_null > 0.40].index
merged.drop(missing_features, axis=1, inplace=True)

# remove useless attribute
del merged['Geography']

merged.drop(missing_features, axis=0, inplace=True)

zip_code = pickle.load(open('/Users/hetianbai/Desktop/DS-GA 1004/1004_project/zipcode.pickle',"rb"))

merged.to_csv('nyc_cencus.csv')

a = zip_code.keys()
ind = []
for index, i in enumerate(merged['Id2']):
    if i not in a:
        ind.append(index)
        
# groupby
merged_new = merged.reset_index(drop=True)
merged_zip = merged_new.groupby(['Id2','Year']).sum()
merged_zip_year = merged_zip.reset_index()
merged_zip_year.to_csv('merged_zip_year.csv')
# finally, remove uncessary columns:
for i in merged_zip_year.columns:
    if 'Error'in i:
        del merged_zip_year[i]

merged_zip_year.to_csv('merged_zip_year.csv')
