
# coding: utf-8

# In[1]:


import pandas as pd


# In[3]:


### Read and transform original datasets
collision_time = pd.read_csv("collision_time.csv")
collision_Year = pd.read_csv("collision_Year.csv")

weather = pd.read_csv("weather.csv")
weather_Year = pd.read_csv("weather_Year.csv")

taxi = pd.read_csv("taxi.csv")
taxi_year = pd.read_csv("taxi_year.csv")

prop_by_year = pd.read_csv("prop_by_year.csv")
prop_by_zip_year = pd.read_csv("prop_by_zip_year.csv")


census_zip_year = pd.read_csv("census_grouped_zip_year.csv")

citibike_time = pd.read_csv("Citibike_group_by_time_cleaned.csv")
citibike_time.drop(['Unnamed: 0'],axis = 1,inplace=True)
citibike_time.rename(columns = {'Processed_start_time':'YearDate'}, inplace = True)
citibike_zip_time = pd.read_csv("Citibike_group_by_zip_and_time_cleaned.csv")
citibike_zip_time.drop(['Unnamed: 0'],axis = 1,inplace=True)
citibike_zip_time.rename(columns = {'Processed_start_time':'YearDate','start_zipcode':'Zip'}, inplace = True)
citibike_zip_time.Zip = citibike_zip_time.Zip.apply(lambda x: int(x))
citibike_year = pd.read_csv("citibike_year.csv")

nypd = pd.read_csv("NYPD_groupby_cleaned.csv")
nypd = pd.read_csv("NYPD_groupby_cleaned.csv")
nypd.drop(['Unnamed: 0'],axis = 1,inplace=True)
nypd.rename(columns = {'Processed_start_time':'YearDate'}, inplace = True)
nypd = nypd.dropna()
nypd.YearDate = nypd.YearDate.apply(lambda x: int(x))
nypd_year = pd.read_csv("nypd_year.csv")
nypd_year["Year"] = nypd_year.Year.apply(lambda x: int(x))




d311 = pd.read_csv("311.csv")
d311 = d311.drop(["Unnamed: 0"],axis = 1)
d311_zip_year = d311[["Year","Zip","total_count"]].groupby(["Year","Zip"]).sum().reset_index()
d311_zip_yeardate = d311[["YearDate","Zip","total_count"]].groupby(["YearDate","Zip"]).sum().reset_index()
d311_yeardate = d311[["YearDate","total_count"]].groupby(["YearDate"]).sum().reset_index()
d311_year = d311[["Year","total_count"]].groupby(["Year"]).sum().reset_index()
d311_zip = d311[["Zip","total_count"]].groupby(["Zip"]).sum().reset_index()


# In[624]:


### Inner join example

match = d311_yeardate.merge(nypd, on = ["YearDate"], how = "inner")
match.to_csv('../Time/d311_nypd_yeardate.csv',index=False)


# # load joined dataset

# In[134]:


weather_collision = pd.read_csv("../Time/weather_collision.csv")

weather_collision.columns


# In[218]:


taxi_weather = pd.read_csv("../Time/taxi_weather.csv")
### slice taxi_weather
taxi_weather = taxi_weather.drop(taxi_weather.index[0:10]).reset_index(drop = True)
idx = taxi_weather.YearDate.apply(lambda x: str(x)[0:4]).isin(["2011","2012","2013","2014","2015","2016"])
taxi_weather = taxi_weather.loc[idx,:]
taxi_weather['YearDate'] = taxi_weather.YearDate.apply(lambda x: pd.to_datetime(str(x)[0:8], format='%Y%m%d'))


# In[220]:


taxi_weather = taxi_weather.groupby('YearDate').agg({'sum_trip_time': 'sum', 
                                                     'mean_trip_time': 'mean',
                                                     'Temp':"mean",
                                                    'sum_pass':'sum',
                                                    'sum_dis':'sum','Spd':'mean',
                                                     'Visb':'mean','Prec':'mean'}).reset_index()


# In[227]:


from plotly.offline import init_notebook_mode, iplot
from plotly.graph_objs import *
import numpy as np

init_notebook_mode(connected=True)         # initiate notebook for offline plot

total_passeger = Scatter(
  x= taxi_weather.YearDate,
  y= 2.5*np.log(taxi_weather.sum_pass)
)

temperature = Scatter(
  x= taxi_weather.YearDate,
  y=taxi_weather.Temp,
)
data = Data([total_passeger,temperature])

iplot(data)    


# # Time vs Weahther and Taxi data

# In[228]:


import plotly 
plotly.tools.set_credentials_file(username='gzmkobe', api_key='5u96tiUm1g9KE2kaOwu4')
import plotly.plotly as py
import plotly.graph_objs as go


# In[328]:


total_passeger = Scatter(
  x= taxi_weather.YearDate,
  y= taxi_weather.sum_pass/100000,
    name='Total Passengers, mutual information = 2.48')


temperature = Scatter(
  x= taxi_weather.YearDate,
  y=taxi_weather.Temp,
    name = "Temperature"
)

layout = go.Layout(title = 'Tempeature vs Total Passengers and Mean Trip Time',
    xaxis=dict(
        title='Time (month)',
        titlefont=dict(
            family='Courier New, monospace',
            size=18,
            color='#7f7f7f'
        )
    ),
    yaxis=dict(
        title='y Axis',
        titlefont=dict(
            family='Courier New, monospace',
            size=18,
            color='#7f7f7f'
        )
    )
)


data = Data([total_passeger,temperature,mean_trip_time])
fig = go.Figure(data=data, layout=layout)
iplot(fig)    


# In[117]:


bins = [-100,-5,0, 5, 10, 15, 20, 25,30,50]
taxi_weather['binned'] = pd.cut(taxi_weather['Temp'], bins)
labels = [0,1,2,3,4,5,6,7,8]
taxi_weather['binned'] = pd.cut(taxi_weather['Temp'], bins=bins, labels=labels)


# # Temp vs total passenger

# In[267]:



d311_weather = pd.read_csv("../Time/d311_weather_yeardate.csv")
d311_weather['YearDate'] = d311_weather.YearDate.apply(lambda x: pd.to_datetime(str(x)[0:8], format='%Y%m%d'))

d311_weather = d311_weather.groupby('YearDate').agg({'total_count': 'sum', 
                                                     'Spd': 'mean',
                                                     'Visb':"mean",
                                                    'Temp':'mean',
                                                     'Prec':'mean'}).reset_index()



bins = [i for i in range(-12,34)]
d311_weather['binned'] = pd.cut(d311_weather['Temp'], bins)
labels = [i for i in range(-12,33)]

d311_weather['binned'] = pd.cut(d311_weather['Temp'], bins=bins, labels=labels)

d311_weather = d311_weather.groupby("binned").agg({'total_count':'mean'}).reset_index()




# In[289]:


Temp_count = go.Scatter(
  x= d311_weather.binned,
  y= d311_weather.total_count,mode = "markers")



data = Data([Temp_count])

iplot(data)    


# In[315]:


Temp_count = go.Scatter(
  x= d311_weather.binned,
  y= d311_weather.total_count,mode = "markers")



data = Data([Temp_count])

iplot(data)    

