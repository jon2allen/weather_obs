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
import re
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
import logging
logger = logging.getLogger('weather_obs_f')

def hunt_for_csv(file_id):
    station_file_list = []
    now = datetime.datetime.now()
    day = str(now.day)
    month = str(now.month)
    dirlist = os.listdir()
    target_csv = ''
    for f in dirlist:
        if( f[:10] == file_id):
          logger.debug("station CSV file: %s" ,f)
          station_file_list.append(f)
          logger.debug("file: %s", f[:10])
    logger.debug(station_file_list)    
    last_hour = 0
    for f in station_file_list:
        m1 = int(f[12:14])
        if m1 == int(now.month):
            logger.debug("match month")
            logger.debug("Month: %s ", f[12:14])
            logger.debug("day: %s ", f[16:18])
            logger.debug("hour: %s ", f[20:22])
            d1 = int(f[16:18])
            if (d1 == int(now.day)):
               logger.debug("Match day: %s", f)
               target_csv = f
            if  d1 < int(day):
               logger.debug("In the past: %s" ,f)
    return target_csv

"""
  function:  parset_date_from_station_csv
  input:  filename ( cannoical format - example 'KDCA_Y2021_M04_D04_H00.csv')
  output:  datetime of file
"""

def parse_date_from_station_csv( fname ):
    csv_name = os.path.split( fname )
    # just need the file name not the path
    ds = re.split('[_.]', str(csv_name[1]))
    year = ds[1]
    year = year[-4:]
    month = ds[2]
    month = month[-2:]
    day = ds[3]
    day = day[-2:]
    return datetime.date(int(year), int(month), int(day))

if __name__ == "__main__":            
           
    parser = argparse.ArgumentParser(description='weather obs - daily chart')
    parser.add_argument('--file', help='name of input file - csv ')
    parser.add_argument('--chart', help='output png file' )
    parser.add_argument('--station', help='station - either linke or 4 char name' )
    parser.add_argument('--table', help='output html table file' )
    parser.add_argument("--tablecols", help='list of cols by position')
    parser.add_argument("--listcols", action="store_true", help='helper func - list columns by position')
    parser.add_argument('--dir', help='director - otherwise /var/www/html/weather_obs' )
    args = parser.parse_args()
    # station is 4 character NOAA observation station in CAPS
    # csv dir is where the data resides
    # where the graph png shoud be placed.
    os.environ['TZ'] = 'US/Eastern'
    if (args.dir):
      print("args.dir: ", args.dir )
      try:
        os.chdir(args.dir)
      except:
        try:
            print ( "trying /var/www/html/weather_obs" ) 
            os.chdir('/var/www/html/weather_obs')
        except:
             print("using start directory")    
    else:
         #todo - doesn't work on windows
         try:
             os.chdir('/var/www/html/weather_obs')
         except:
             pass
    station = ""
    csv_dir = ""
    graph_out_dir = ""
    now = datetime.datetime.now()

    day = str(now.day)
    month = str(now.month)
    print("month: ", month )
    print("day: ", day )

    chart_date = now.strftime("%b %d, %Y") 
    print("chart_date: ", chart_date )
   
    if (args.file):
       dt = parse_date_from_station_csv( args.file )
       chart_date = dt.strftime("%b %d, %Y")
       print(" new chart_date: ", chart_date )
   

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
    print("target_csv: ", target_csv)           

    if (args.file):
        target_csv = str(args.file)
        print("file input: ", target_csv)
    
    try:        
        date_utc = lambda x: dateutil.parser.parse(x, ignoretz=True)
        obs1 = pd.read_csv(target_csv,parse_dates=[9],date_parser=date_utc)
    except:
        print("file not found:  ", target_csv)
        exit(16)

    if( args.listcols ):
        x = 0
        for cols in obs1.columns:
            print("column: ", x, "  -- " , cols)
            x = x+1
        sys.exit(0) 
    # default tablecols for wind
    table_col_list =  [9, 19, 17, 21, 16]
    if (args.tablecols):
        try:
           table_col_list = list(map(int, args.tablecols.split(',')))  
        except:
            print("html table list not column intergers")

             
    
    #print(obs1.shape)
    #print(obs1.columns)

    obs1['wind_gust_mph'] = pd.to_numeric(obs1['wind_gust_mph'], errors = 'coerce')
    obs1['wind_mph'] = pd.to_numeric(obs1['wind_mph'], errors = 'coerce')
    # obs1['observation_time'] = obs1['observation_time'].dt.hour

    # print(obs1.dtypes)


    # rows, cols = wind.shape
    positions = []
    labels = []
    fig, ax = plt.subplots()
    fig.set_size_inches(12,6)


    x = obs1['observation_time']
    y = obs1['wind_mph']
    z = obs1['wind_dir']
    chart_loc = obs1['location']
    print("chart_loc: ", chart_loc[1])

    ax.plot_date( x,y, linestyle = "solid")
    plt.grid(True)
    plt.title( str(chart_loc[1]) + " - " + chart_date , fontsize=15)
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
    
    # todo - make subroutine and pass list of columns by number from --tablecols
    # 9, 19, 17, 12 
    def weather_obs_html_table( obs_col, file_f):
        try:
            obs_prn = obs1.iloc[:, obs_col ]
            out_text = obs_prn.to_html()
        except:
            print("column out of range or other")
            return False
        try:
            file_html = open(file_f, 'w') 
        except:
            print("error - cannot open", file_f )
            return False
        file_html.write(out_text)
        print( out_text )
        file_html.close()
        return True
   
    if ( args.table):
        weather_obs_html_table( table_col_list, args.table)
        #default
    else:
        weather_obs_html_table( table_col_list , 'wind_chart.html')
    
    
    
