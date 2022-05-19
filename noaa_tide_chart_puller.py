#!/usr/bin/python
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
#from freezegun import freeze_time
#import unittest

#freezer = freeze_time("2021-12-30 23:56:30", tick=True)
#freezer.start()

class ObsCollector:
    def __init__(self, url, id):
        self.station_url = url
        self.station_id = id

    def show_collector(self):
        return str(self.station_id + "@" + self.station_url)

    def get_url_data(self):
        self.url_data = requests.get(self.station_url)

    def show_url_data(self):
        """show Url data if desired"""
        return self.url_data.text

    def set_station_file_name(self):
        self.obs_filename = create_station_file_name(self.station_id, "txt")

    def write_station_data(self):
        """write out last forcast"""
        obs_file = open(self.obs_filename, "w")
        obs_file.write(self.last_forcast)
        obs_file.close()

    def write_station_data_custom(self, ending_text):
        obs_file = open(self.station_id + "_" + ending_text, "w")
        obs_file.write(self.last_forcast)
        obs_file.close()

    def obs_collection_sequence(self):
        """ basic collection sequence - fetch, intpret, write """
        self.get_url_data()
        self.find_station_data()
        self.set_station_file_name()
        if (self.obs_collection_duplicate_check()):
            print("duplicate:  exit")
        else:
            self.write_station_data()

    def obs_collection_duplicate_check(self):
        # note to get correct md5 checksum - binary file comparison
        # initialz test_md5 - preven error on fresh dir
        test_md5 = " "
        self.write_station_data_custom("dupcheck.txt")
        with open(self.station_id + "_" + "dupcheck.txt", "rb") as fl:
            blob1 = fl.read()
            curr_md5 = str(hashlib.md5(blob1).hexdigest())
            print("Curr_md5:", curr_md5)
        today = datetime.now()
        day_1 = timedelta(hours=24)
        yesterday = today - day_1
        today_glob = obs_utils.create_station_glob_filter(
            self.station_id, "txt", today)
        yesterday_glob = obs_utils.create_station_glob_filter(
            self.station_id, "txt", yesterday)
        last_file = obs_utils.hunt_for_noaa_files2(".", today_glob)
        if len(last_file) < 1:
            last_file = obs_utils.hunt_for_noaa_files2(".", yesterday_glob)
        if (len(last_file) > 0):
            with open(last_file, "rb") as f2:
                blob2 = f2.read()
                test_md5 = str(hashlib.md5(blob2).hexdigest())
                print("test_md5", test_md5)
                print("test_file ", last_file)
        return (curr_md5 == test_md5)

    def find_station_data(self):
        """ parse station data from noaa html page """
        f = BeautifulSoup(self.url_data.text, 'lxml')
        my_content = f.find(id="contentarea")
        # print(my_content)
        t = re.finditer(r'\w\w\w\d\d\d\W\d\d\d\d\d\d',  str(my_content))
        station_list = []
        text_obs = str(my_content)
        #leantext = BeautifulSoup(text_obs,'lxml').text
        #text_obs = leantext
        for i in t:
            index = int(i.start())
            # print(str(index))
            # print(text_obs[index:(index+6)])
            station_name = text_obs[index:(index+6)]
            station_list.append((index, station_name))
        tail = re.finditer(r'Start Tail', str(my_content))
        for s in tail:
            # print("Start Tail: ", str(s.start()))
            station_list.append((int(s.start() - 5), 'end'))
        print(station_list)
        print(len(station_list))
        station_list_len = len(station_list)
        # print("test_obs:")
        # print(text_obs)

        for i in range(station_list_len):
            station_data = "<not found>"
            if i == (station_list_len - 1):
                break
            #print("i: ", str(i) )
            station_data = text_obs[station_list[i][0]:station_list[i+1][0]]
            #print("station_data:", station_data)
            #print(" self.station:", self.station_id)
            if self.station_id in station_data:
                my_station = station_data
                break
        r_station_data = BeautifulSoup(station_data, 'lxml').text
        self.last_forcast = r_station_data
        print(hashlib.md5(r_station_data.encode('utf-8')).hexdigest())
        return r_station_data

#
#  init the collector
#  run the collection sequnce
#  1 additional custom write out of the data

class ObsTideCollector( ObsCollector):
    def __init__( self, url, id ):
        super().__init__(url, id)
        # use cache on disk first
        # need test for end of year
        # may need to get this year and next.
        # merge df together. 
        # need to write out to customre file - 
        # alex_Y2022_data.txt
        # need new functino that gets and reads this file.
        # append this df_2022 to the df in the class
        today = datetime.now()
        match = re.search('bdate=(\d+)', self.station_url)
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
    def find_station_data(self):
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
                              parse_dates = [0], sep = '\s+', header = 12)
        #self.df = self.df.drop(columns = ['Time', 'Pred(Ft)', 'Pred(cm)'])
        print('my_df')

        my_df['Date'] = my_df.apply(lambda x: x['Date'][:-8], axis = 1)
        my_df['tide_time'] = my_df['Day'] + my_df['Time']
        my_df = my_df.drop(columns = ['Date', 'Time', 'Day'])
        if self.df.empty:
            self.df = my_df
        else:
            self.df = self.df.append(my_df, ignore_index=False)
    def get_last_tidal_data(self):
        today = datetime.now()
        day_7 = timedelta(hours=168)
        Seven_days = today - day_7
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
                              parse_dates = [0], sep = '\s+', header = 12)
        #my_df = my_df.df.drop(columns = ['Time', 'Pred(Ft)', 'Pred(cm)'])
        print('my_df')

        my_df['Date'] = my_df.apply(lambda x: x['Date'][:-8], axis = 1)
        my_df['tide_time'] = my_df['Day'] + my_df['Time']
        my_df = my_df.drop(columns = ['Date', 'Time', 'Day'])
        if self.df.empty:
            self.df = my_df
        else:
            self.df = self.df.append(my_df, ignore_index=False)
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
        self.find_station_data()
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
        tide_table = self.df.loc[str(today): str(enddate)]
        tide_table.index.name = "Date"
        print("write tide table")
        print(tide_table.head(10)) 
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

        #tide_table.to_html('alex_tide_table.html', header=None)  


# TODO - check for duplicate - save output and check with MD5 on next run
# html to noaa marine forcase page - change to what is require

if __name__ == "__main__":
    
#######################################################################
# FORCASTURL is where the data resides
# FORCASTID is the station forcast ( upper tidal potomac is ANZ535
# The script changes wording directory and then writes
# DATA_DIR
#######################################################################
    TIDECHARTURL = 'https://tidesandcurrents.noaa.gov/cgi-bin/predictiondownload.cgi?&stnid=8594900&threshold=&thresholdDirection=greaterThan&bdate=2021&timezone=LST/LDT&datum=MLLW&clock=12hour&type=txt&annual=true'
    FORECASTID = 'alex'
    DATA_DIR = '/var/www/html/weather_obs/data'

    # change to your desirect dirctory
    try:
        os.chdir(DATA_DIR)
    except:
        pass

# thinking - logic - collect xml once a week.
#              each day - write out html for tide - current day + 3
#            plot?
    import logging
    logger = logging.getLogger('weather_obs_f')

    obs_t = ObsTideCollector(url=TIDECHARTURL, id=FORECASTID)
    print(obs_t.show_collector())


    # write out to "latest" for page pickup
    # saves effort on figuring out which file to open

    print("obs_t")

    obs_t.obs_collection_sequence()
    
    print(obs_t.obs_filename)
    
    print(obs_t.df.head(40))
    print(obs_t.df.columns)
    print(obs_t.df.shape)
    print(obs_t.df.describe())
    print(obs_t.df.dtypes)
    #print(obs_t.get_next_year_data())
    #obs_t.read_next_year_data()
    print(obs_t.df.shape)
    print(obs_t.df.tail(1400))
    obs_t.df.to_csv('alex_test.csv', mode ="w")
    
    sys.exit()
