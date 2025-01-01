#!/usr/bin/env python3
#########################################################################
# NOAA marine tidal chart collector
#
# Scrapes data from NOAA marine tidal chart site
#
# meant to be run periodically - once per day.
#  data is cached once per week and refreshed.
#  tidal forecast is "well known" in advance.
#
# output - html file with tidal forcast .
#
#########################################################################

import os,sys
import hashlib
import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
# from weather_obs import *
import obs_utils
from weather_obs import create_station_file_name
from io import StringIO
import pandas as pd
from obs_3_day_collect import ObsCollector
#from freezegun import freeze_time
#import unittest

#freezer = freeze_time("2021-12-30 23:56:30", tick=True)
#freezer.start()

#  init the collector
#  run the collection sequnce
#  1 additional custom write out of the data

class ObsTideCollector( ObsCollector):
    def __init__( self, url, id, filetype ):
        super().__init__(url, id, filetype )
        # use cache on disk first
        # need test for end of year
        # may need to get this year and next.
        # merge df together. 
        # need to write out to customre file - 
        # alex_Y2022_data.txt
        # need new functino that gets and reads this file.
        # append this df_2022 to the df in the class
        today = datetime.now()
        match = re.search(r'bdate=(\d+)', self.station_url)
        print(f'match group 1 {match.group(1)}')
        bstring = "bdate=" + str(match.group(1))
        nstring = "bdate=" + str(today.year)
        print("bstring:", bstring)
        print("nstring: ", nstring)
        print(f'match group 1 {match.group(1)}')
        self.station_url = self.station_url.replace(bstring,nstring)
        self.df = pd.DataFrame()
        f_list = self.get_last_tidal_data()
        print("f_list:", f_list)
        if len(f_list) > 0:
            self.obs_filename = f_list.pop()
            self.load_tidal_data(self.obs_filename)
            self.read_next_year_data()
    def _find_station_data(self):
        self.last_forcast = str(self.url_data.text)
        return self.url_data
    def obs_collection_duplicate_check(self):
        # may just find the last xml file
        # if less than period, return TRUE
        # so every 7 days for tide.?? user setting???
        file_list = self.get_last_tidal_data()
        if len(file_list) > 0:
            print("dup found")
            print( file_list)
            return True
        return False
    def load_tidal_data(self, tidal_file):
        my_df = pd.read_csv(tidal_file, index_col=0,
                              parse_dates = [0], date_format="%Y/%m/%d  %a", sep = r'\s+', header = 12)
        #self.df = self.df.drop(columns = ['Time', 'Pred(Ft)', 'Pred(cm)'])
        print('my_df')
        my_df['Date'] = my_df.apply(lambda x: x['Date'][:-8], axis = 1)
        my_df['tide_time'] = my_df['Day'] + my_df['Time']
        my_df = my_df.drop(columns = ['Date', 'Time', 'Day'])
        if self.df.empty:
            self.df = my_df
        else:
            # self.df = self.df.append(my_df, ignore_index=False)
            self.df = pd.concat([self.df, my_df], ignore_index=False)
    def get_last_tidal_data(self):
        today = datetime.now()
        day_7 = timedelta(hours=168)
        Seven_days = today - day_7
        if Seven_days.year != today.year:
           print("Seven day before start of year")
           return []
        file_list = obs_utils.gather_any_noaa_files('.', self.station_id, 'txt', Seven_days, today)
        return file_list
    def set_station_file_name(self):
        self.obs_filename = create_station_file_name(self.station_id, "txt")
    def obs_collection_sequence(self):
        if self.df.empty:
            super().obs_collection_sequence()
            self.get_next_year_data()
            self.read_next_year_data() 
            self.write_tide_table_to_html()
        else:
            self.read_next_year_data()
            self.write_tide_table_to_html()

    def get_url_data(self):
        super().get_url_data()
        print("tide get url")
        my_df = pd.read_csv(StringIO(self.url_data.text), index_col=0,
                              parse_dates = [0], date_format="%Y/%m/%d  %a", sep = r'\s+', header = 12)
        #my_df = my_df.df.drop(columns = ['Time', 'Pred(Ft)', 'Pred(cm)'])
        print('my_df')
        my_df['Date'] = my_df.apply(lambda x: x['Date'][:-8], axis = 1)
        my_df['tide_time'] = my_df['Day'] + my_df['Time']
        my_df = my_df.drop(columns = ['Date', 'Time', 'Day'])
        if self.df.empty:
            self.df = my_df
        else:
            # self.df = self.df.append(my_df, ignore_index=False)
            self.df = pd.concat([self.df, my_df], ignore_index=False)
        #index = self.df.index
        #print(index)
    def get_next_year_data(self):
        today = datetime.now()
        next_year = today.year + 1
        bstring = "bdate=" + str(today.year)
        nstring = "bdate=" + str(next_year)
        print("bstring:", bstring)
        print("nstring: ", nstring)
        self.station_url = self.station_url.replace(bstring,nstring)
        print("url: ", self.station_url)
        self.get_url_data()
        self._find_station_data()
        self.write_station_data_custom("Y"+str(next_year)+".txt")
    def read_next_year_data(self):
        today = datetime.now()
        next_year = today.year + 1
        next_year_file = self.station_id + "_Y" + str(next_year) + ".txt"
        self.load_tidal_data( next_year_file)
    def write_tide_table_to_html(self):
        today = datetime.now().date()
        three_days = timedelta(hours=72)
        enddate = (today + three_days)
        print(str(today))
        print(str(enddate))
        #tide_table = self.df['2021-09'].iloc[:, :6]
        #tide_table = self.df.loc[str(today): str(enddate)]
        tide_table = self.df.loc[str(today.strftime('%Y/%m/%d')): str(enddate.strftime('%Y/%m/%d'))] 
        #tide_table = self.df.loc[today:enddate]
        tide_table.index.name = "Date"
        print("write tide table")
        #print(tide_table.head(10)) 
        print(tide_table.columns)

        html_doc = tide_table.to_html(header=False, justify='right')

        #html_doc2 = tide_table.to_html()
        print(html_doc)
        soup = BeautifulSoup(html_doc, 'html.parser')

        empty_cols = soup.find('thead').find_all(lambda tag: not tag.contents)

        for tag, col in zip(empty_cols, tide_table):
            tag.string = col

        with open(self.station_id + "_tide_table.html", "w") as f:
            f.write(soup.decode_contents())
            
        print(tide_table.head(10))

        #tide_table.to_html('alex_tide_table.html', header=None)  


# TODO - check for duplicate - save output and check with MD5 on next run
# html to noaa marine forcase page - change to what is require

def debug_out(obs_t, debug ):
    if debug:
        print(obs_t.df.head(40))
        print(obs_t.df.columns)
        print(obs_t.df.shape)
        print(obs_t.df.describe())
        print(obs_t.df.dtypes)
        #print(obs_t.get_next_year_data())
        #obs_t.read_next_year_data()
        print(obs_t.df.shape)
        print(obs_t.df.tail(1400))

if __name__ == "__main__":
    
#######################################################################
# FORCASTURL is where the data resides
# FORCASTID is the station forcast ( upper tidal potomac is ANZ535
# The script changes wording directory and then writes
# DATA_DIR
#######################################################################
    TIDECHARTURL = 'https://tidesandcurrents.noaa.gov/cgi-bin/predictiondownload.cgi?&stnid=8594900&threshold=&thresholdDirection=greaterThan&bdate=2023&timezone=LST/LDT&datum=MLLW&clock=12hour&type=txt&annual=true'
    FORECASTID = 'alex'
    DATA_DIR = '/var/www/html/weather_obs/data'
    DEBUG=False

    # change to your desirect dirctory
    try:
        os.chdir(DATA_DIR)
    except Exception:
        pass 

# thinking - logic - collect xml once a week.
#              each day - write out html for tide - current day + 3
#            plot?
    import logging
    logger = logging.getLogger('weather_obs_f')

    obs_t = ObsTideCollector(url=TIDECHARTURL, id=FORECASTID, filetype="txt")
    print(obs_t.show_collector())


    # write out to "latest" for page pickup
    # saves effort on figuring out which file to open

    print("obs_t")

    obs_t.obs_collection_sequence()
    
    print(obs_t.obs_filename)
    
    debug_out(obs_t, DEBUG)
    obs_t.df.to_csv('alex_test.csv', mode ="w")
    
    sys.exit()
