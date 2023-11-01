#!/usr/bin/env python3
#########################################################################
# NOAA marine forecast collector
#
# Scrapes data from NOAA marine forecast site
#
# meant to be run periodically - say every 2 to 12 hours
#
# output - text file with forcast
#
#########################################################################

import os,sys
import hashlib
import re
from datetime import datetime, timedelta
from xml.dom.minidom import Attr
import requests
from bs4 import BeautifulSoup
# from weather_obs import *
import obs_utils
from weather_obs import create_station_file_name
from obs_3_day_collect import ObsCollector


class MarineForcast(ObsCollector):
    def __init__(self, url, id, filetype):
        super().__init__(url, id, filetype)

    def _find_station_data(self):
        """ parse station data from noaa html page """
        #f = BeautifulSoup(self.url_data.text, 'lxml')
        soup = BeautifulSoup(self.url_data.text, "html.parser")

        # Find all the <pre> tags in the page
        pre_tags = soup.find_all("p")
        r_station_data = ""
        station_flag = False
        warn_flag = False
        # Loop through each <pre> tag
        for pre_tag in pre_tags:
            tag_class = pre_tag.get("class", [])
            #print("tag_class:", tag_class)
            try:
                if tag_class[0] == "warn-highlight":
                    warn_flag = True
                    print("forcast has warning")
            except:
                pass
        # Get the text content of the tag
            text = pre_tag.get_text()
            # Split the text into lines
            lines = text.split("\n")
            if warn_flag == True:
                lines.append("\n")
                warn_flag = False
            # print("lines:" , lines)
            # Loop through each line
            for line in lines:
                # print("l:", line)
                # Check if the line starts with ANZxxx
                if line.startswith("ANZ"):
                   if line.startswith(self.station_id) is False:
                        print("end of station data...")
                        station_flag = False
                        break
                if line.startswith(self.station_id):
                    station_flag = True
                if station_flag == True:
                    print(line)
                    r_station_data = r_station_data + line + "\n"
                    if line.endswith("."):
                       r_station_data = r_station_data + "\n"
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
    
#######################################################################
# FORCASTURL is where the data resides
# FORCASTID is the station forcast ( upper tidal potomac is ANZ535
# The script changes wording directory and then writes
# DATA_DIR
#######################################################################
    FORECASTURL = 'https://www.ndbc.noaa.gov/data/Forecasts/FZUS51.KLWX.html'
    FORECASTID = 'ANZ535'
    DATA_DIR = '/var/www/html/weather_obs/data'

    # change to your desirect dirctory
    try:
        os.chdir(DATA_DIR)
    except:
        pass

    import logging
    logger = logging.getLogger('weather_obs_f')

    obs_x = MarineForcast(
        url=FORECASTURL, id=FORECASTID, filetype='txt')

    print(obs_x.show_collector())

    obs_x.obs_collection_sequence()

    print(obs_x.obs_filename)
    # write out to "latest" for page pickup
    # saves effort on figuring out which file to open
    obs_x.write_station_data_custom("latest.txt")


    sys.exit()
