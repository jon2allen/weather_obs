#!/usr/bin/python3
"""
 Program:  daily_weather_obs_chart
 Author:   Jon Allen
 Purpose:  To take NOAA weather csv and display current day data
 Command args:   
          
"""
import logging
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
import json
from obs_utils import trendline, read_weather_obs_csv, parse_date_from_station_csv

"""
  This will find the last hour of the current day
  if you want a specific file - specify --file
"""
logger = logging.getLogger('weather_obs_f')


def hunt_for_csv(file_id):
    station_file_list = []
    now = datetime.datetime.now()
    day = str(now.day)
    month = str(now.month)
    dirlist = os.listdir()
    target_csv = ''
    for f in dirlist:
        if(f[:10] == file_id):
            if 'csv' in f:
                logger.debug("station CSV file: %s", f)
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
            if d1 < int(day):
                logger.debug("In the past: %s", f)
    return target_csv


"""
  function:  parset_date_from_station_csv
  input:  filename ( cannoical format - example 'KDCA_Y2021_M04_D04_H00.csv')
  output:  datetime of file
"""




def weather_obs_subset(obs1, obs_col):
    """ returns a subset of data """
    try:
        obs_subset = obs1.iloc[:, obs_col]
        return obs_subset
    except:
        print("column out of range or other")
        return obs1


def weather_obs_html_table(obs1, obs_col, file_f):
    try:
        obs_prn = obs1.iloc[:, obs_col]
        out_text = obs_prn.to_html()
    except:
        print("column out of range or other")
        return False
    try:
        file_html = open(file_f, 'w')
    except:
        print("error - cannot open", file_f)
        return False
    file_html.write(out_text)
    print(out_text)
    file_html.close()
    return True


def draw_obs_wind_chart(chart_date, fig_png, obs1):
    """ draw chart to png file """
    print ("bshape", obs1.shape)
    obs1.drop(obs1.index[obs1['wind_mph'] == "<no_value_provided>"], inplace = True)
    obs1 = obs1.reset_index(drop=True)
    print("shape", obs1.shape)
    obs1['wind_gust_mph'] = pd.to_numeric(
        obs1['wind_gust_mph'], errors='coerce')
    obs1['wind_mph'] = pd.to_numeric(obs1['wind_mph'], errors='coerce')
    positions = []
    labels = []
    fig, ax = plt.subplots()
    fig.set_size_inches(12, 6)
    x = obs1['observation_time']
    y = obs1['wind_mph']
    z = obs1['wind_dir']
    chart_loc = obs1['location']
    print("chart_loc: ", chart_loc[0])
    ax.plot_date(x, y, linestyle="solid")
    plt.grid(True)
    plt.title(str(chart_loc[0]) + " - " + chart_date, fontsize=15)
    print("xlim: ", ax.get_xlim())
    print("ylim: ", ax.get_ylim())
    print("len y ", len(y))
    print("len x ", len(x)," ",  x.size )
    print(x)
    print(y)
    for i in range(x.size):
        if (i == (x.size - 1)):
            ax.annotate(z[i], (mdates.date2num(x[i]), y[i]),
                        xytext=(-15, -15), textcoords='offset pixels')
        else:
            if y[i] == y[i+1]:
                if (len(z[i]) > 7):
                    ax.annotate(z[i], (mdates.date2num(x[i]), y[i]), xytext=(15, -35), ha='center', va='center', rotation=315,
                                textcoords='offset pixels')
                else:
                    ax.annotate(z[i], (mdates.date2num(x[i]), y[i]), xytext=(-30, -15),
                                textcoords='offset pixels')
            else:
                ax.annotate(z[i], (mdates.date2num(x[i]), y[i]), xytext=(-15, -15),
                            textcoords='offset pixels')
    fig.autofmt_xdate()
    fig.text(0.04, 0.5, 'Wind Speed - MPH', va='center',
             rotation='vertical', fontsize=18)
    fig.text(0.5, 0.05,  'Hour of day', va='center', fontsize=18)
    if (len(y) > 2 ):
         date_ser = mdates.date2num(x)
         tr = trendline(date_ser, y)
         fig.text(0.1, 0.05, 'Polyfit: {:8.4f}'.format(tr), va='center', fontsize=16)
    # plt.xticks(positions,labels)
    date_form = DateFormatter("%I")
    ax.xaxis.set_major_formatter(date_form)
    print(x)
    print(y)
    print(z)
    print(x.size)
    print(y.size)
    fig.savefig(fig_png, dpi=fig.dpi)
    return True


def obs_meta_date_json(station, obs1):
    date_ser = mdates.date2num(json_out['observation_time'])
    y = json_out['wind_mph']
    obs_data = {}
    obs_data['station'] = station
    obs_data['polyfit'] = 0
    if len(y) > 1:
       obs_data['polyfit'] = trendline(date_ser, y)
    obs_data['max'] = obs1['wind_mph'].max()
    obs_data['min'] = obs1['wind_mph'].min()

    json_out2 = json.dumps(obs_data)
    return json_out2


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='weather obs - daily chart')
    parser.add_argument('--file', help='name of input file - csv ')
    parser.add_argument('--chart', help='output png file')
    parser.add_argument(
        '--station', help='station - either linke or 4 char name')
    parser.add_argument('--table', help='output html table file')
    parser.add_argument("--tablecols", help='list of cols by position')
    parser.add_argument("--listcols", action="store_true",
                        help='helper func - list columns by position')
    parser.add_argument(
        '--dir', help='director - otherwise /var/www/html/weather_obs')
    args = parser.parse_args()
    # station is 4 character NOAA observation station in CAPS
    # csv dir is where the data resides
    # where the graph png shoud be placed.
    os.environ['TZ'] = 'US/Eastern'
    if (args.dir):
        print("args.dir: ", args.dir)
        try:
            os.chdir(args.dir)
        except:
            try:
                print("trying /var/www/html/weather_obs")
                os.chdir('/var/www/html/weather_obs')
            except:
                print("using start directory")
    else:
        # todo - doesn't work on windows
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
    print("month: ", month)
    print("day: ", day)

    chart_date = now.strftime("%b %d, %Y")
    print("chart_date: ", chart_date)

    if (args.file):
        dt = parse_date_from_station_csv(args.file)
        chart_date = dt.strftime("%b %d, %Y")
        print(" new chart_date: ", chart_date)

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
    fig_png = station + '_current' + '.png'

    if (args.chart):
        fig_png = str(args.chart)

    print("file_id:", file_id)

    target_csv = hunt_for_csv(file_id)
    print("target_csv: ", target_csv)

    if (args.file):
        target_csv = str(args.file)
        print("file input: ", target_csv)
        station = target_csv[0:4]
        print("station:", station)

    obs1 = read_weather_obs_csv(target_csv)

    if(args.listcols):
        x = 0
        for cols in obs1.columns:
            print("column: ", x, "  -- ", cols)
            x = x+1
        sys.exit(0)
    # default tablecols for wind
    # df[['observation_time','wind_mph','wind_dir','wind_gust_mph','wind_string']]
    table_col_list = [9, 19, 17, 21, 16]
    if (args.tablecols):
        try:
            table_col_list = list(map(int, args.tablecols.split(',')))
        except:
            print("html table list not column intergers")


    if (args.table):
        weather_obs_html_table(obs1, table_col_list, args.table)
        # default
    else:
        weather_obs_html_table(obs1,  table_col_list, 'wind_chart.html')


    # print(obs1.shape)

    # print(obs1.columns)
    print ("bshape", obs1.shape)
    obs1.drop(obs1.index[obs1['wind_mph'] == "<no_value_provided>"], inplace = True)
    obs1 = obs1.reset_index(drop=True)
    print("shape", obs1.shape)

    obs1['wind_gust_mph'] = pd.to_numeric(
        obs1['wind_gust_mph'], errors='coerce')
    obs1['wind_mph'] = pd.to_numeric(obs1['wind_mph'], errors='coerce')


    obs2 = obs1.copy(deep=True)
    draw_obs_wind_chart(chart_date, fig_png, obs2)



    # print(ax.axis())
    # date series.

    json_out = weather_obs_subset(obs1, table_col_list)
    result = json_out.to_json(orient="split", date_format="iso")
    parsed = json.loads(result)
    print(json.dumps(parsed, indent=4))

    date_ser = mdates.date2num(json_out['observation_time'])
    y = json_out['wind_mph']

    print("len date_ser: ", str(len(date_ser)))
    print("len wind_mph(y): ", str(len(y)))
    print(date_ser)
    print(y)
    if (len(y) > 1 ):
         print("polyfit: ", str(trendline(date_ser, y)))
    print("Max wind speed: ", str(json_out['wind_mph'].max()))
    print("Min wind speed: ", str(json_out['wind_mph'].min()))

    json_out2 = obs_meta_date_json(station, obs1)

    print(json_out2)
