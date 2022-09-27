#!/usr/bin/python
#########################################################################
# 3 day collector
#
# Scrapes data from station 3 day
#
#
#
# output - csv with 3 day data
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
from weather_obs import create_station_file_name, obs_sanity_check
import pandas as pd
import numpy as np



class ObsCollector:
    def __init__(self, url, id, filetype):
        self.station_url = url
        self.station_id = id
        self.filetype = filetype
        self.allowdup = True
        self.appendflag = True
        self.obsday = 0

    def show_collector(self):
        return str(self.station_id + "@" + self.station_url)

    def get_url_data(self):
        self.url_data = requests.get(self.station_url)

    def show_url_data(self):
        """show Url data if desired"""
        return self.url_data.text

    def set_station_file_name(self):
        if not self.obs_filename:
            self.obs_filename = create_station_file_name(self.station_id, self.filetype)

    def _write_station_data( self, fname):
        obs_file = open(fname, "w")
        obs_file.write(self.last_forcast)
        obs_file.close()
    
    def _append_station_data( self, fname):
        obs_file = open(fname, "a")
        obs_file.write(self.last_forcast)
        obs_file.close()
    
    def _transform_station_data( self):
        pass
    
    def _post_process_station_data( self ):
        pass
    
    def _pre_process_station_data( self ):
        pass
        
    def write_station_data(self):
        """write out last forcast"""
        if self.appendflag == True:
           self._append_station_data( self.obs_filename)
        else:
           self._write_station_data(self.obs_filename)

    def write_station_data_custom(self, ending_text):
        self._write_station_data(self.station_id + "_" + ending_text ) 


    def obs_collection_sequence(self):
        """ basic collection sequence - fetch, intpret, write """
        self._pre_process_station_data()
        self.get_url_data()
        self._find_station_data()
        self.set_station_file_name()
        self._transform_station_data()
        if (self.obs_collection_duplicate_check()):
            print("duplicate:  exit")
        else:
            self.write_station_data()
        self._post_process_station_data()
        return

    def obs_collection_duplicate_check(self):
        # note to get correct md5 checksum - binary file comparison
        # initialz test_md5 - preven error on fresh dir
        print("base class")
        if self.allowdup == True:
            return False
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
            self.station_id, self.filetype, today)
        print("today_glob: ", today_glob )
        yesterday_glob = obs_utils.create_station_glob_filter(
            self.station_id, self.filetype, yesterday)
        last_file = obs_utils.hunt_for_noaa_files3(".", today_glob, self.filetype)
        print("last file: ", last_file)
        if len(last_file) < 1:
            last_file = obs_utils.hunt_for_noaa_files3(".", yesterday_glob, self.filetype)
        if (len(last_file) > 0):
            with open(last_file, "rb") as f2:
                blob2 = f2.read()
                test_md5 = str(hashlib.md5(blob2).hexdigest())
                print("test_md5", test_md5)
                print("test_file ", last_file)
        return (curr_md5 == test_md5)

    def _find_station_data(self):
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

class ObsCollector3day( ObsCollector):
    def _find_station_data(self):
        #print(self.url_data.text)
        tables1 = pd.read_html( self.url_data.text)
        obs_a = tables1[3]
        obs_a.columns = obs_a.columns.get_level_values(2)
        #print(obs_a.head())
        self.last_forcast = obs_a
        return obs_a
    
    def _write_station_data(self, fname):
        self.last_forcast.to_csv( fname, index=False, header=True )

    def _append_station_data(self, fname):
        self.last_forcast.to_csv( fname, index=False) 
        
    def obs_collection_duplicate_check(self):
        print("I am here in 3 day")
        return super().obs_collection_duplicate_check()
    
class ObsCollector3dayhourly( ObsCollector3day):
    def _find_station_data(self):
        print("hourly")
        obs1 = super()._find_station_data()
        obs_c = obs1.drop( obs1.index[1:obs1.shape[0]])
        print(obs_c)
        self.last_forcast = obs_c
        return obs_c
    
    def _append_station_data(self, fname):
        self.last_forcast.to_csv( fname, header=False, index=False , mode='a' )
        return
    
    def _post_process_station_data(self):
        self.obsday = self.last_forcast['Date'].values[0] 
        print( f"Current date: { self.obsday } ")
        return 
    
    def _pre_process_station_data(self):
        now = datetime.now()
        g1 = obs_utils.create_station_glob_filter("KDCA_3_day", "csv", now)
        print(f"G1: {g1}" )
        target = obs_utils.hunt_for_noaa_files3( '.', g1, 'csv' )
        print(f"target: { target }")
        if len(target) < 3:
            self.appendflag = False
            self.allowdup = False
        else:
            self.appendflag = True
            self.allowdup = True
            self.obs_filename = target
        
        return 

#    def obs_collection_sequence(self):
#       """ basic collection sequence - fetch, intpret, write """
#        self.get_url_data()
#        self.find_station_data()
#        self.set_station_file_name()
#        if (self.obs_collection_duplicate_check()):
#            print("duplicate:  exit")
#        else:
#            self.write_station_data()
#        return    
#
#  init the collector
#  run the collection sequnce
#  1 additional custom write out of the data


# TODO - check for duplicate - save output and check with MD5 on next run
# html to noaa marine forcase page - change to what is require

if __name__ == "__main__":
    
#######################################################################
# FORCASTURL is where the data resides
# FORCASTID is the station forcast ( upper tidal potomac is ANZ535
# The script changes wording directory and then writes
# DATA_DIR
#######################################################################
    FORECASTURL = 'https://w1.weather.gov/data/obhistory/KDCA.html'
    FORECASTID = 'KDCA_3_day'
    DATA_DIR = '/var/www/html/weather_obs/data'

    # change to your desirect dirctory
    try:
        os.chdir(DATA_DIR)
    except:
        pass

    import logging
    logger = logging.getLogger('weather_obs_f')

    obs_x = ObsCollector3dayhourly(
        url=FORECASTURL, id=FORECASTID, filetype='csv')

    print(obs_x.show_collector())

    obs_x.obs_collection_sequence()

    print(obs_x.obs_filename)
    # write out to "latest" for page pickup
    # saves effort on figuring out which file to open
    obs_x.write_station_data_custom("latest.csv")


    sys.exit()
