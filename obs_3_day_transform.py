#!/usr/bin/env python3

import os
import sys
import hashlib
import re
from datetime import datetime, timedelta
import calendar
import requests
from bs4 import BeautifulSoup
# from weather_obs import *
import obs_utils
from weather_obs import create_station_file_name, obs_sanity_check
import pandas as pd
import numpy as np
import obs_3_day_collect
import obs_time
import csv


class ThreeDayTransform:
    def __init__(self, df3):
        self.df3 = df3
        self.calm = False
        self.vrbl = False
        self.wind_parts = self.df3['Wind(mph)'].values[0].split()
        if len(self.wind_parts) < 3:
            self.gust = False
        else:
            self.gust = True
        if self.wind_parts[0] == "Calm":
            self.calm = True
        if self.wind_parts[0] == "Vrbl":
            self.vrbl = True
        print("wind_parts: ", self.wind_parts)
        self.transform_dict = {
            'credit':  ['text', "NOAA's National Weather Service"],
            'credit_URL': ['text', "https://weather.gov/"],
            'image': ['text', "                                                                      "],
            'suggested_pickup': ['text', "15 minutes after the hour"],
            'suggested_pickup_period': ['text', "60"],
            'location': ['text', "Washington/Reagan National Airport, DC, VA"],
            'station_id': ['text', "KDCA"],
            'latitude': ['text', "38.84833"],
            'longitude': ['text', "-77.03417"],
            'observation_time': ['func',  self.get_observation_time],
            'observation_time_rfc822': ['func', self.get_observation_time_rfc],
            'weather': ['func', self.get_weather_statement],
            'wind_string': ['func', self.get_wind_str],
            'wind_dir': ['func', self.get_wind_dir],
            'wind_degrees': ['func', self.get_wind_degrees],
            'wind_mph': ['func', self.get_wind_speed],
            'wind_kt': ['func', self.get_wind_knots],
            'temp_f': ['func', self.get_temp_F],
            'temp_c': ['func', self.get_temp_C],
            'temperature_string': ['func', self.get_temp_str],
            'relative_humidity': ['func', self.get_humidity],
            'wind_gust_mph': ['func', self.get_gust_speed],
            'wind_gust_kt': ['func', self.get_gust_knots],
            'pressure_mb': ['func', self.get_pressure_mb],
            'pressure_in': ['func', self.get_pressure_in],
            'pressure_string': ['func', self.get_pressure_str],
            'dewpoint_f': ['func', self.get_dewpoint_f],
            'dewpoint_c': ['func', self.get_dewpoint_c],
            'dewpoint_string': ['func', self.get_dewpoint_str],
            'heat_index_f': ['func', self.get_heatindex_f],
            'windchill_f': ['func', self.get_windchill_f],
            'heat_index_c': ['func', self.get_windchill_c],
            'windchill_c':  ['func', self.get_windchill_c],
            'heat_index_string': ['func', self.get_heatindex_str],
            'windchill_string': ['func', self.get_windchill_str],
            'visibility_mi': ['func', self.get_visiblity],
            'icon_url_base': ['text',
                              "https://forecast.weather.gov/images/wtf/small/"],
            'two_day_history_url': ['text',
                                    "https://www.weather.gov/data/obhistory/KDCA.html"],
            "ob_url": ['text',
                       "https://www.weather.gov/data/METAR/KDCA.1.txt"],
            'icon_url_name': ['text', 'some.png'],
            'disclaimer_url': ['text',
                               "https://www.weather.gov/disclaimer.html"],
            'copyright_url': ['text', "https://www.weather.gov/disclaimer.html"],
            'privacy_policy_url': ['text', "https://www.weather.gov/notice.html"]

        }

    def get_visiblity(self):
        return self.df3['Vis.(mi.)'].values[0]

    def get_weather_statement(self):
        return self.df3['Weather'].values[0]

    def get_observation_time(self):
        _Year = self.df3['Year'].values[0]
        _Month = self.df3['Month'].values[0]
        _day = self.df3['Date'].values[0]
        _time = self.df3['Time'].values[0]
        d_1 = datetime.strptime(_time, "%H:%M")
        _cal_month = calendar.month_abbr[int(_Month)]

        try:
            format1 = f'{_cal_month} {_day} {_Year}, {d_1.strftime("%-I:%M %p")} EDT'
        except ValueError:
            format1 = f'{_cal_month} {_day} {_Year}, {d_1.strftime("%#I:%M %p")} EDT'

        return format1

    def get_observation_time_rfc(self):
        date1 = obs_time.ObsDate(self.get_observation_time())
        print("date1: ", date1)
        date1.emit_type('rfc')
        print(date1)
        return date1

    def get_wind_speed(self):
        if self.calm:
            return "0"
        else: 
            return self.wind_parts[1]

    def get_wind_knots(self):
        return obs_utils.knots(float(self.get_wind_speed()))

    def get_wind_dir(self):
        if self.calm:
            return "North"
        if self.vrbl:
            return "Variable"
        else:
            return obs_utils.wind_text(self.wind_parts[0])

    def get_wind_degrees(self):
        return obs_utils.cardinal_points(self.get_wind_dir())

    def get_wind_str(self):
        current_dir = obs_utils.wind_text(self.get_wind_dir())
        current_wind_speed = self.get_wind_speed().strip()
        current_wind_knots = self.get_wind_knots()
        if self.gust == True:
            current_gust = self.get_gust_speed().strip()
            current_gust_knots = str(self.get_gust_knots()).strip()
            format1 = f"from the {current_dir} at {current_wind_speed} gusting to {current_gust}"
            format1 = format1 + f" MPH ({current_wind_knots} KT gusting to {current_gust_knots} KT)"
        else:
            format1 = f"{current_dir} at {current_wind_speed} MPH ({current_wind_knots} KT)"
        return format1

    def get_gust_speed(self):
        if self.gust == False:
            return obs_null_value
        if len(self.wind_parts) > 2:
            return self.wind_parts[3]

    def get_gust_knots(self):
        if self.gust == False:
            return obs_null_value
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

    def get_dewpoint_f(self):
        return self.df3['Dwpt'].values[0]

    def get_dewpoint_c(self):
        return ((float(self.get_dewpoint_f()) - 32) * .5556)

    def get_dewpoint_str(self):
        dew_f = self.get_dewpoint_f()
        dew_c = self.get_dewpoint_c()
        format1 = f"{dew_f} F ({dew_c} C)"
        return format1

    def get_windchill_f(self):
        if self.df3["WindChill(°F)"].isnull().values.any():
            return obs_null_value
        return self.df3["WindChill(°F)"].values[0]

    def get_windchill_c(self):
        if self.df3["WindChill(°F)"].isnull().values.any():
            return obs_null_value
        return ((float(self.get_windchill_f()) - 32) * .5556)

    def get_windchill_str(self):
        wc_f = self.get_windchill_f()
        if wc_f == obs_null_value:
            return obs_null_value
        wc_c = self.get_windchill_c()
        format1 = f'{wc_f} F ({wc_c}C)'
        return format1

    def get_heatindex_f(self):
        if self.df3["HeatIndex(°F)"].isnull().values.any():
            return obs_null_value
        return self.df3["HeatIndex(°F)"].values[0]

    def get_heatindex_c(self):
        if self.df3["HeatIndex(°F)"].isnull().values.any():
            return obs_null_value
        return ((float(self.get_heatindex_f()) - 32) * .5556)

    def get_heatindex_str(self):
        hi_f = self.get_heatindex_f()
        if hi_f == obs_null_value:
            return obs_null_value
        hi_c = self.get_heatindex_c()
        format1 = f'{hi_f} F ({hi_c}C)'
        return format1

    def process_transform(self):
        global csv_headers
        df1 = pd.DataFrame()
        for hd1 in csv_headers[::-1]:
            print(hd1)
            item = self.transform_dict[hd1]
            if item[0] == 'text':
                df1.insert(0, hd1, [item[1]])
            if item[0] == 'func':
                df1.insert(0, hd1, [item[1]()])
        print(df1.columns)
        return df1
# loop through CSV headers
# create empty DF.  use df.insert(0, 'column_name', 'value' from transform functin) to add these.


csv_headers = ['credit', 'credit_URL', 'image', 'suggested_pickup', 'suggested_pickup_period', 'location', 'station_id', 'latitude', 'longitude', 'observation_time', 'observation_time_rfc822', 'weather', 'temperature_string', 'temp_f', 'temp_c', 'relative_humidity', 'wind_string', 'wind_dir', 'wind_degrees', 'wind_mph', 'wind_kt', 'wind_gust_mph',
               'wind_gust_kt', 'pressure_string', 'pressure_mb', 'pressure_in', 'dewpoint_string', 'dewpoint_f', 'dewpoint_c', 'heat_index_string', 'heat_index_f', 'heat_index_c', 'windchill_string', 'windchill_f', 'windchill_c', 'visibility_mi', 'icon_url_base', 'two_day_history_url', 'icon_url_name', 'ob_url', 'disclaimer_url', 'copyright_url', 'privacy_policy_url']

three_day_headers = ['Date', 'Time', 'Wind(mph)', 'Vis.(mi.)', 'Weather', 'Sky Cond.', 'Air', 'Dwpt', 'Max.', 'Min.',
                     'RelativeHumidity', 'WindChill(°F)', 'HeatIndex(°F)', 'altimeter(in)', 'sea level(mb)', '1 hr', '3 hr', '6 hr']

obs_null_value = "<no_value_provided>"

transform_dict = {
    'credit':  ['text', "NOAA's National Weather Service"],
    'credit_URL': ['text', "https://weather.gov/"],
    'image': ['text', "                                                                      "],
    'suggested_pickup': ['text', "15 minutes after the hour"],
    'suggested_pickup_period': ['text', "60"],
    'location': ['text', "Washington/Reagan National Airport, DC, VA"],
    'station': ['text', "KDCA"],
    'latitude': ['text', "38.84833"],
    'longitude': ['text', "-77.03417"],
    'observation_time': ['func',  ThreeDayTransform.get_observation_time]

}


if __name__ == "__main__":

    FORECASTURL = 'https://w1.weather.gov/data/obhistory/KDCA.html'
    FORECASTID = 'KDCA_3_day'
    DATA_DIR = '/var/www/html/weather_obs/data'

    mydata = obs_3_day_collect.ObsCollector3dayhourly(
        FORECASTURL, FORECASTID, 'csv')

    mydata.writeflag = False
    mydata.obs_collection_sequence()

    print(mydata.last_forcast)

    transformer = ThreeDayTransform(mydata.last_forcast)

    transformer.get_observation_time()

    my_df = transformer.process_transform()

    my_df.to_csv('transform.csv', index=False, header=True,
                 quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
