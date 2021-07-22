#!/usr/bin/python
import os,sys
import hashlib
import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
# from weather_obs import *
import obs_utils
from weather_obs import create_station_file_name


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


# TODO - check for duplicate - save output and check with MD5 on next run
# html to noaa marine forcase page - change to what is require

if __name__ == "__main__":

    tidal_pt = 'https://www.ndbc.noaa.gov/data/Forecasts/FZUS51.KLWX.html'

    # change to your desirect dirctory
    try:
        os.chdir('/var/www/html/weather_obs')
    except:
        pass

    import logging
    logger = logging.getLogger('weather_obs_f')

    obs_x = ObsCollector(
        url='https://www.ndbc.noaa.gov/data/Forecasts/FZUS51.KLWX.html', id="ANZ535")

    print(obs_x.show_collector())

    obs_x.obs_collection_sequence()

    print(obs_x.obs_filename)
    # write out to "latest" for page pickup
    # saves effort on figuring out which file to open
    obs_x.write_station_data_custom("latest.txt")


    sys.exit()
