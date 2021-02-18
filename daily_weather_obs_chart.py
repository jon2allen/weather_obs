#!/usr/bin/python3
"""
 Program:  daily_weather_obs_chart
 Author:   Jon Allen
 Purpose:  To take NOAA weather csv and display current day data
 Command args:   
          
"""           
import sys
import os
import argparse
import csv
import pandas as pd
import numpy as np
import time
# station is 4 character NOAA observation station in CAPS
# csv dir is where the data resides
# where the graph png shoud be placed.
station = ""
csv_dir = ""
graph_out_dir = ""
day = "16"
month = "02"

"""
  code logic
  1.  find all files with current month.
  2.  open and see if equal to date, if not move on to next csv for month until no more
  3.  process data for chart
      - temp wind and guust
"""

station = "KDCA"

dirlist = os.listdir()  
station_file_list = []

file_id = station + "_Y"

for f in dirlist:
    if( f[:6] == file_id):
       print("station CSV file:" ,f)
       station_file_list.append(f)
    print("file: ", f[:6])
print(station_file_list)    
for f in station_file_list:
    print("Month: ", f[12:14])
    print("day: ", f[16:18])
    d1 = int(f[16:18])
    if (f[16:18] == day):
        print("Match day:", f)
    if  d1 < int(day):
        print("In the past:" ,f)