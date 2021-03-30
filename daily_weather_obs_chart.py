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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import time
import datetime
import dateutil

"""
  This will find the last hour of the current day
  if you want a specific file - specify --file
"""
def hunt_for_csv(file_id):
    station_file_list = []
    dirlist = os.listdir()
    target_csv = ''
    for f in dirlist:
        if( f[:10] == file_id):
          print("station CSV file:" ,f)
          station_file_list.append(f)
          print("file: ", f[:10])
    print(station_file_list)    
    last_hour = 0
    for f in station_file_list:
        m1 = int(f[12:14])
        if m1 == int(now.month):
            print("match month")
            print("Month: ", f[12:14])
            print("day: ", f[16:18])
            print("hour:  ", f[20:22])
            d1 = int(f[16:18])
            if (d1 == int(now.day)):
               print("Match day:", f)
               target_csv = f
            if  d1 < int(day):
               print("In the past:" ,f)
    return target_csv
            
           
parser = argparse.ArgumentParser(description='weather obs - daily chart')
parser.add_argument('--file', help='name of input file - csv ')
parser.add_argument('--chart', help='output png file' )
parser.add_argument('--station', help='station - either linke or 4 char name' )
args = parser.parse_args()

# station is 4 character NOAA observation station in CAPS
# csv dir is where the data resides
# where the graph png shoud be placed.
os.environ['TZ'] = 'US/Eastern'
try:
    os.chdir('/var/www/html/weather_obs')
except:
    print("using start directory")    
station = ""
csv_dir = ""
graph_out_dir = ""
now = datetime.datetime.now()

day = str(now.day)
month = str(now.month)
print("month: ", month )
print("day: ", day )


"""
  code logic
  1.  find all files with current month.
  2.  open and see if equal to date, if not move on to next csv for month until no more
  3.  process data for chart
      - temp wind and guust
"""

if (args.station):
    station = args.station
    print("station: ", station)
else:
    station = "KDCA"

dirlist = os.listdir()  
station_file_list = []
target_csv = ""

file_id = station + "_Y" + str(now.year)
fig_png  = station + '_current'+ '.png'


if (args.chart):
   fig_png = str(args.chart)

print("file_id:", file_id )

         
target_csv = hunt_for_csv(file_id)
print("test_csv: ", target_csv)           

if (args.file):
    target_csv = str(args.file)
    print("file input: ", target_csv)
   
try:        
    date_utc = lambda x: dateutil.parser.parse(x, ignoretz=True)
    obs1 = pd.read_csv(target_csv,parse_dates=[9],date_parser=date_utc)
except:
    print("file not found:  ", target_csv)
    exit(16)

print(obs1.shape)
print(obs1.columns)

obs1['wind_gust_mph'] = pd.to_numeric(obs1['wind_gust_mph'], errors = 'coerce')
obs1['wind_mph'] = pd.to_numeric(obs1['wind_mph'], errors = 'coerce')
# obs1['observation_time'] = obs1['observation_time'].dt.hour

print(obs1.dtypes)


# rows, cols = wind.shape
positions = []
labels = []
fig, ax = plt.subplots()
fig.set_size_inches(12,6)


x = obs1['observation_time']
y = obs1['wind_mph']
z = obs1['wind_dir']

ax.plot_date( x,y, linestyle = "solid")
plt.grid(True)
plt.title("National Airport", fontsize=14)
for  i in range(  x.size   ):
     if (i == (x.size - 1 )):
             ax.annotate(z[i], (mdates.date2num(x[i]), y[i]), xytext=(-15, -15), 
               textcoords='offset points')
     else:
           if y[i] == y[i+1]:
                ax.annotate(z[i], (mdates.date2num(x[i]), y[i]), xytext=(-30, 15), 
                   textcoords='offset points')
           else:
                ax.annotate(z[i], (mdates.date2num(x[i]), y[i]), xytext=(-15, -15), 
                    textcoords='offset points')


fig.autofmt_xdate()
fig.text(0.04,0.5, 'Wind Speed - MPH', va='center', rotation='vertical', fontsize=18)
fig.text(0.5,0.05,  'Hour of day', va='center', fontsize=18)
# plt.xticks(positions,labels)
date_form = DateFormatter("%I")
ax.xaxis.set_major_formatter(date_form)
print(x)
print(y)
print(z)
print(x.size)
print(y.size)        
fig.savefig(fig_png, dpi=fig.dpi)


obs_prn = obs1[['observation_time','wind_mph','wind_dir','wind_gust_mph','wind_string']]


out_txt = obs_prn.to_html()

print(out_txt)

file_html = open('wind_chart.html', 'w')
file_html.write(out_txt)
file_html.close()

 
