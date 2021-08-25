# from daily_weather_obs_chart import read_weather_obs_csv
import os
import sys
import glob
import datetime
import dateutil
from datetime import timedelta
from pathlib import Path
import functools
import time
import csv
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter

import logging
logger = logging.getLogger('weather_obs_f')


def read_weather_obs_csv(target_csv):
    """ read csv and return dataframe """
    try:
        # ignore time zone for parse here - times local to observation
        def date_utc(x): return dateutil.parser.parse(x[:20], ignoretz=True)
        obs1 = pd.read_csv(target_csv, parse_dates=[9], date_parser=date_utc)
    except:
        print("file not found:  ", target_csv)
        exit(16)
    return obs1


def trendline(index, data, order=1):
    coeffs = np.polyfit(index, list(data), order)
    slope = coeffs[-2]
    return float(slope)


def create_station_glob_filter(station='ANZ535', ext='txt', obs_time_stamp=0):
    """ 
    create station glob filter from current time or time provided
    ignore hour
    for use with NOAA marine forcast data
    """
    if (obs_time_stamp == 0):
        t_now = datetime.datetime.now()
    else:
        t_now = obs_time_stamp
    year, month, day, hour, min = map(
        str, t_now.strftime("%Y %m %d %H %M").split())
    file_n = station + '_Y' + year + '_M' + month + '_D' + day + '_H'
    return file_n


def create_station_monthly_glob_filter(station='ANZ535', ext='txt', gyear=2021, gmonth=0):
    """ 
    create station glob filter from current time or time provided
    ignore day and hour
    for use with NOAA marine forcast data
    """
    if (gmonth == 0):
        # current month.
        t_now = datetime.datetime.now()
    else:
        t_now = datetime.date(gyear, gmonth, 1)
    year, month, day, hour, min = map(
        str, t_now.strftime("%Y %m %d %H %M").split())
    file_n = station + '_Y' + year + '_M' + month + '_D' + "*." + str(ext)
    return file_n


def create_station_yearly_glob_filter(station='ANZ535', ext='txt', gyear=2021):
    """ 
    create station glob filter from current time or time provided
    ignore day and hour
    for use with NOAA marine forcast data
    """
    file_n = create_station_monthly_glob_filter(station, ext, gyear, 1)
    yearly_glob = file_n.replace('M01_D', "M")
    return yearly_glob


def get_obs_csv_files(dir, noaa_station):
    glob_path = Path(dir)
    file_list = [str(pp) for pp in glob_path.glob(str(noaa_station + "*.csv"))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def get_noaa_text_files(dir, noaa_station):
    glob_path = Path(dir)
    file_list = [str(pp) for pp in glob_path.glob(str(noaa_station+"*.txt"))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def diff_month(d1, d2):
    # stolen from Stackoverflow
    # https://stackoverflow.com/questions/4039879/best-way-to-find-the-months-between-two-dates
    return (d1.year - d2.year) * 12 + d1.month - d2.month


def gather_any_noaa_files(dir, noaa_station, ext, startdt, enddt):
    # startdt, enddt are datetime.
    # only accept month/days - hours not accepted here.
    # logic will be if same month - then get all the month and filter.
    # if months different - calc difference in months.
    # then gather all the months, then filter.
    pass


def gather_monthly_noaa_files(dir, noaa_station, ext, tyear, tmonth):
    glob_filter = create_station_monthly_glob_filter(
        noaa_station, ext, tyear, tmonth)
    glob_path = Path(dir)
    file_list = [str(pp) for pp in glob_path.glob(str(glob_filter))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def load_monthly_noaa_csv_files(dir, noaa_station, ext="csv", tyear=2021, tmonth=0):
    file_list = gather_monthly_noaa_files(
        dir, noaa_station, ext, tyear, tmonth)
    month_df = pd.DataFrame()
    if len(file_list) > 0:
        for f in file_list:
            obs1 = read_weather_obs_csv(f)
            print(obs1.head())
            month_df = month_df.append(obs1, ignore_index=True)
    return month_df


def hunt_for_noaa_files2(dir, station_glob_filter):
    """
    pass in a glob filter - it can be the station name. or a sub-filter
    version #2 has to be used with a less greedy filter.
    also - doesn't matter if you transition a year/month
    have to glob for that up frong.
    """
    target_csv = ""
    station_file_list = get_noaa_text_files(dir, station_glob_filter)
    if (len(station_file_list) > 0):
        target_csv = station_file_list[-1]
    return target_csv


def hunt_for_noaa_csv_files(dir, station_glob_filter):
    """
    pass in a glob filter - it can be the station name. or a sub-filter
    version #2 has to be used with a less greedy filter.
    also - doesn't matter if you transition a year/month
    have to glob for that up frong.
    """
    target_csv = ""
    station_file_list = get_obs_csv_files(dir, station_glob_filter)
    if (len(station_file_list) > 0):
        target_csv = station_file_list[-1]
    return target_csv


def hunt_for_noaa_files(dir, station_glob_filter):
    """
    pass in a glob filter - it can be the station name. or a sub-filter
    """
    station_file_list = get_noaa_text_files(dir, station_glob_filter)
    now = datetime.datetime.now()
    day = str(now.day)

    month = str(now.month)
    #
    target_csv = ''
    for f in station_file_list:
        # print("file: ", f)
        if (f.find('latest') > 1) or (f.find('dupcheck') > 1):
            continue
        f_station, f_year, f_month, f_day, f_hour = f.split("_")
        m1 = int(f_month[1:3])
        d1 = int(f_day[1:3])
        h1 = int(f_day[1:3])
        # print( "m1,d1,h1", str(m1), " ", str(d1), " ", str(h1))
        if m1 == int(now.month):
            logger.debug("match month")
            logger.debug("Month: %s ", str(m1))
            logger.debug("day: %s ", str(d1))
            logger.debug("hour: %s ", str(h1))
            if (d1 == int(now.day)):
                logger.debug("Match day: %s", f)
                target_csv = f
            if d1 < int(day):
                # will return the oldest found
                logger.debug("In the past: %s", f)
                target_csv = f
    return target_csv


def construct_daily_cmd_call(file, obs_dir):
    myfile = file.split('.')
    c_file = " --file " + file
    c_chart = " --chart " + myfile[0] + ".png"
    c_table = " --table " + myfile[0] + ".html"
    c_dir = " --dir " + obs_dir
    cmd = r"python " + obs_dir + os.sep + \
        r"daily_weather_obs_chart.py" + c_file + c_chart + c_table + c_dir
    return cmd


if __name__ == "__main__":

    import logging
    logger = logging.getLogger('weather_obs_f')

    print("testing csv files")
    # change for unix
    obs_dir = r"C:\Users\jonallen\Documents\github\weather_obs"
    #obs_dir = r"/var/www/html/weather_obs"
    flist = get_obs_csv_files(obs_dir, "")

    print(flist)

    mycmd = construct_daily_cmd_call("KDCA_Y2021_M02_D19_H15.csv", obs_dir)

    print("contruct command test:")
    print(mycmd)

    print("NOAA files")
    print("")
    first_test = hunt_for_noaa_files(".", "ANZ535")

    print("1st:  ", first_test)

    print("hunting")

    # - means - create for now/today "Dxx_H"
    tst4 = create_station_glob_filter("ANZ535", "txt", 0)
    tst5 = create_station_glob_filter("KDCA", "csv", 0)

    print("tst4 ", tst4)
    print("tst5 ", tst5)

    second_tst = hunt_for_noaa_files(".", tst4)
    third_tst = hunt_for_noaa_csv_files(".", tst5)

    print(second_tst)
    print("third")
    print(third_tst)

    now = datetime.datetime.now()
    today = str(now.day)
    day_delta = datetime.timedelta(hours=24)
    prior_day = now - day_delta
    yesterday = str(prior_day.day)

    print("today:", today)
    print("yesterday:", yesterday)

    g1 = create_station_glob_filter("ANZ535", "txt", now)
    g2 = create_station_glob_filter("ANZ535", "txt", prior_day)

    print("g1:", g1)
    print("g2: ", g2)

    g3 = hunt_for_noaa_files2(".", g1)
    g4 = hunt_for_noaa_files2(".", g2)

    print(g3)
    print(g4)

    month1 = create_station_monthly_glob_filter("ANZ535", "txt", 2021, 4)
    month2 = create_station_monthly_glob_filter("ANZ535", "txt", 2021, 0)
    month3 = create_station_monthly_glob_filter("KDCA", "csv", 2021, 0)
    year1 = create_station_yearly_glob_filter("KDCA", "csv", 2021)
    print("month filter: ", month1)
    print("month filter (now): ", month2)
    print("month filter (now/KDCA): ", month3)
    print("yearly filter: ", year1)

    print(gather_monthly_noaa_files(".", "KDCA", "csv", 2021, 4))
    print(gather_monthly_noaa_files(".", "KDCA", "csv", 2021, 9))

    obs2 = load_monthly_noaa_csv_files(".", "KDCA", "csv", 2021, 4)

    print(obs2.shape)

    print(obs2.head(150))

    # obs2.to_csv("month_test.csv", index=False)

    T1 = datetime.date(2020, 11, 1)
    T2 = datetime.date(2021, 8, 1)

    print("diff_month: ", diff_month(T1, T2))
