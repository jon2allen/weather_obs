
from asyncio.proactor_events import _ProactorBaseWritePipeTransport
import os,sys
import hashlib
import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
# from weather_obs import *
import obs_utils
from weather_obs import create_station_file_name, obs_sanity_check
import pandas as pd
import numpy as np
import obs_3_day_collect

class ThreeDayTransform:
    def __init__(self, df3):
        self.df3 = df3
        self.wind_parts = self.df3['Wind(mph)'].values[0].split()
        print("wind_parts: ", self.wind_parts)
        self.transform_dict = {
            'credit':  [ 'text', "NOAA's National Weather Service" ],
            'credit_URL' : ['text', "https://weather.gov/" ],
            'observation_time': [ 'func',  self.get_observation_time ],
            'wind_dir' : [ 'func', self.get_wind_dir ] ,
            'wind_degrees' : ['func', self.get_wind_degrees ],
            'wind_mph': [ 'func', self.get_wind_speed ],
            'wind_kt' : [ 'func', self.get_wind_knots ],
            'temp_f' : ['func', self.get_temp_F],
            'temp_c' : ['func', self.get_temp_C],
            'temprature_string': ['func', self.get_temp_str],
            'relative_humidity': [ 'func', self.get_humidity ],
            'wind_gust': ['func', self.get_gust_speed ],
            'wind_gust_kt': ['func', self.get_gust_knots ],
            'pressure_mb' : ['func', self.get_pressure_mb ],
            'pressure_in' : ['func', self.get_pressure_in ],
            'pressure_string' : ['func', self.get_pressure_str ]
        }
    def get_observation_time(self):
        print ( self.df3['Year'].values[0], '/', self.df3['Month'].values[0])
    
    def get_wind_speed(self):
        return self.wind_parts[1]
    
    def get_wind_knots(self):
        return obs_utils.knots(float(self.get_wind_speed()))
    
    def get_wind_dir(self):
        return self.wind_parts[0]
    
    def get_wind_degrees(self):
        return obs_utils.cardinal_points(self.get_wind_dir())
    
    def get_gust_speed(self):
        if len(self.wind_parts) > 2:
            return self.wind_parts[3]
        
    def get_gust_knots(self):
        return obs_utils.knots(float(self.get_gust_speed()))
    
    def get_temp_F(self):
        return self.df3['Air'].values[0]
    
    def get_temp_C(self):
        return ((float(self.get_temp_F()) - 32) * .5556)
    
    def get_temp_str(self):
        # 69.1 F (20.6 C)
        temp_f = self.get_temp_F()
        temp_c = self.get_temp_C()
        return f"{ temp_f} F ({temp_c} C)"
    
    def get_humidity(self):
        return self.df3['RelativeHumidity'].values[0] 
    
    def get_pressure_in(self):
        return self.df3["altimeter(in)"].values[0]
    
    def get_pressure_mb(self):
        return self.df3["sea level(mb)"].values[0]
    
    def get_pressure_str(self):
        return self.get_pressure_mb() + " mb"
  
        
    def process_transform(self):
        for key in self.transform_dict:
            mylist = self.transform_dict[key]
            if mylist[0] == 'text':
                print( mylist[0])
                print( mylist[1])
            if mylist[0] == 'func':
                print( mylist[0])
                x = mylist[1]() 
                print(x)
# loop through CSV headers                
# create empty DF.  use df.insert(0, 'column_name', 'value' from transform functin) to add these.

csv_headers = ['credit', 'credit_URL', 'image', 'suggested_pickup', 'suggested_pickup_period', 'location', 'station_id', 'latitude', 'longitude', 'observation_time', 'observation_time_rfc822', 'weather', 'temperature_string', 'temp_f', 'temp_c', 'relative_humidity', 'wind_string', 'wind_dir', 'wind_degrees', 'wind_mph', 'wind_kt', 'wind_gust_mph',
               'wind_gust_kt', 'pressure_string', 'pressure_mb', 'pressure_in', 'dewpoint_string', 'dewpoint_f', 'dewpoint_c', 'heat_index_string', 'heat_index_f', 'heat_index_c', 'windchill_string', 'windchill_f', 'windchill_c', 'visibility_mi', 'icon_url_base', 'two_day_history_url', 'icon_url_name', 'ob_url', 'disclaimer_url', 'copyright_url', 'privacy_policy_url']

three_day_headers = ['Date','Time(edt)','Wind(mph)','Vis.(mi.)','Weather','Sky Cond.','Air','Dwpt','Max.','Min.','RelativeHumidity','WindChill(°F)','HeatIndex(°F)','altimeter(in)','sea level(mb)','1 hr','3 hr','6 hr']

obs_null_value = "<no_value_provided>"

transform_dict = {
    'credit':  [ 'text', "NOAA's National Weather Service" ],
    'credit_URL' : ['text', "https://weather.gov/" ],
    'image': ['text', "                                                                      " ],
    'suggested_pickup' : [ 'text', "15 minutes after the hour"],
    'suggested_pickup_period' : [ 'text', "60"],
    'location' : [ 'text', "Washington/Reagan National Airport, DC, VA"],
    'station' : ['text', "KDCA"], 
    'latitude' : ['text', "38.84833" ],
    'longitude' : ['text', "-77.03417" ],
    'observation_time': [ 'func',  ThreeDayTransform.get_observation_time ]
            
}


if __name__ == "__main__":
    
    FORECASTURL = 'https://w1.weather.gov/data/obhistory/KDCA.html'
    FORECASTID = 'KDCA_3_day'
    DATA_DIR = '/var/www/html/weather_obs/data'

    mydata = obs_3_day_collect.ObsCollector3dayhourly(FORECASTURL, FORECASTID, 'csv')
    
    mydata.writeflag = False
    mydata.obs_collection_sequence()
    
    print(mydata.last_forcast)
    
    transformer = ThreeDayTransform( mydata.last_forcast)
    
    transformer.get_observation_time()
    
    transformer.process_transform()



