#!/usr/bin/env python3
#########################################################################
# 3 day collector
#
# Scrapes data from station 3 day
# Made to be called hourly on :20
#  Each new entry at 1st row written into csv with stationid prefix
#  At midnight - new csv will be created.
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
import csv
from obs_3_day_collect import ObsCollector3day, ObsCollector3dayhourly
import obs_3_day_transform
 

class ObsCollector3dayWithTransform(ObsCollector3dayhourly):
    def __init__( self, url, id, filetype):
        super().__init__(url, id, filetype)
    def _transform_station_data(self):
        transformer = obs_3_day_transform.ThreeDayTransform(self.last_forcast)
        my_df = transformer.process_transform()
        self.last_forcast = my_df
        return 
    
    def _post_process_station_data(self):
        pass
        
    
    
if __name__ == "__main__":
    
#######################################################################
# FORCASTURL is where the data resides
# FORCASTID is the station forcast 
# The script changes wording directory and then writes
# DATA_DIR
#######################################################################
    FORECASTURL = 'https://w1.weather.gov/data/obhistory/KDCA.html'
    FORECASTID = 'KDCA_3_day_csv'
    DATA_DIR = '/var/www/html/weather_obs/data'

    # change to your desirect dirctory
    try:
        os.chdir(DATA_DIR)
    except:
        pass

    import logging
    logger = logging.getLogger('weather_obs_f')

    obs_x = ObsCollector3dayWithTransform(
        url=FORECASTURL, id=FORECASTID, filetype='csv')

    print(obs_x.show_collector())
    
    obs_x.row_num = 5

    obs_x.obs_collection_sequence()

    print(obs_x.obs_filename)
    # write out to "latest" for page pickup
    # saves effort on figuring out which file to open
    # obs_x.write_station_data_custom("latest.csv")


    sys.exit()
