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


import logging
logger = logging.getLogger('weather_obs_f')
trace = True


def trace_print(level, first_string, *optional_strings):
    """ central logging function """
    global trace
    global logger
    trace_out = first_string + ''.join(optional_strings)
    if (trace == True):
        if (level == 1):
            logger.debug(trace_out)
        elif (level == 2):
            logger.critcal(trace_out)
        elif (level == 3):
            logger.warning(trace_out)
        elif (level == 4):
            logger.info(trace_out)
        else:
            print("level not known:  ", trace_out, flush=True)


def read_weather_obs_csv(target_csv):
    """ read csv and return dataframe """
    # handle no_value_provided as NAN
    dtype_dict = {'temp_f': np.float64,
                  'temp_c': np.float64,
                  'wind_mph': np.float64,
                  'wind_kt': np.float64,
                  'wind_gust': np.float64,
                  'wind_gust_kt': np.float64,
                  'pressure_mb': np.float64,
                  'pressure_in': np.float64,
                  'dewpoint_f': np.float64,
                  'dewpoint_c': np.float64,
                  'heat_index_f': np.float64,
                  'heat_index_c': np.float64,
                  'windchill_f': np.float64,
                  'windchill_c': np.float64,
                  'latitude': 'category',
                  'longitude': 'category',
                  'suggested_pickup_period': 'category',
                  'credit': 'category',
                  'credit_URL': 'category',
                  'icon_url_base': 'category',
                  'two_day_history_url': 'category',
                  'icon_url_name': 'category',
                  'ob_url': 'category',
                  'disclaimer_url': 'category',
                  'copyright_url': 'category',
                  'privacy_policy_url': 'category'
                  }

    try:
        # ignore time zone for parse here - times local to observation
        def date_utc(x): return dateutil.parser.parse(x[:20], ignoretz=True)
        obs1 = pd.read_csv(target_csv, parse_dates=[9], date_parser=date_utc,
                           dtype=dtype_dict,
                           na_values="<no_value_provided>")
    except OSError:
        trace_print(4, "file not found:  ", target_csv)
        # return empty dataframe
        obs1 = pd.DataFrame()
    return obs1


def trendline(index, data, order=1):
    coeffs = np.polyfit(index, list(data), order)
    slope = coeffs[-2]
    return float(slope)


def month_to_num(my_month):
    _month = {	'january': 1,
               'february': 2,
               'march': 3,
               'april': 4,
               'may': 5,
               'june': 6,
               'july': 7,
               'august': 8,
               'september': 9,
               'october': 10,
               'november': 11,
               'december': 12	}
    return _month[my_month]


def parse_date_from_station_csv(fname):
    csv_name = os.path.split(fname)
    # just need the file name not the path
    ds = re.split('[_.]', str(csv_name[1]))
    for item in ds:
        if item[0:1] == 'Y':
            year = item[-4:]
        if item[0:1] == 'M':
            month = item[-2:]
        if item[0:1] == 'D':
            day = item[-2:]
    return datetime.date(int(year), int(month), int(day))


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
    # get csv only
    glob_path = Path(dir)
    file_list = [str(pp) for pp in glob_path.glob(str(noaa_station + "*.csv"))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def get_noaa_text_files(dir, noaa_station):
    # get txt files only
    glob_path = Path(dir)
    file_list = [str(pp) for pp in glob_path.glob(str(noaa_station+"*.txt"))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def get_noaa_any_files(dir, noaa_station, ext):
    #  Generic get any ext
    glob_path = Path(dir)
    file_list = [str(pp)
                 for pp in glob_path.glob(str(noaa_station + '*.' + ext))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def diff_month(d1, d2):
    # stolen from Stackoverflow
    # https://stackoverflow.com/questions/4039879/best-way-to-find-the-months-between-two-dates
    return abs((d1.year - d2.year) * 12 + d1.month - d2.month)


def get_next_month_year(month, year):
    i_month = month
    i_year = year
    first_pass = True
    while True:
        if i_month == 12:
            i_month = 1
            i_year = i_year + 1
        else:
            i_month = i_month+1
        if first_pass:
            i_month = month
            i_year = year
            first_pass = False
        yield (i_month, i_year)


def file_exclusion_filter(f_name):
    f_list = ['Range', 'Month', 'Year']
    for f in f_list:
        if f_name.find(f) > 0:
            return True
    return False


def gather_any_noaa_files(dir, noaa_station, ext, startdt, enddt):
    # startdt, enddt are datetime.
    # only accept month/days - hours not accepted here.
    # logic will be if same month - then get all the month and filter.
    # if months different - calc difference in months.
    # then gather all the months, then filter.
    range_list = gather_monthly_noaa_files(
        dir, noaa_station, ext, startdt.year, startdt.month)

    num_months = diff_month(startdt, enddt)
    print("num_months: ", num_months)
    if num_months > 1:
        m_generator = get_next_month_year(startdt.month, startdt.year)
        for n_month in range(num_months + 1):
            next_month = next(m_generator)
            trace_print(4, "  searching year: ", str(
                next_month[1]), " month: ", str(next_month[0]))
            month_list = gather_monthly_noaa_files(
                dir, noaa_station, ext, next_month[1], next_month[0])
            if len(month_list) > 0:
                range_list = range_list + month_list
    final_list = []
    startdt = startdt.date()
    enddt = enddt.date()
    while range_list:
        csv_file = range_list.pop(0)
        if file_exclusion_filter(csv_file):
            continue
        try:
            f_date = parse_date_from_station_csv(csv_file)
        except ValueError:
            trace_print(4, "bad file name:", csv_file)
        if (f_date >= startdt) and (f_date <= enddt):
            final_list.append(csv_file)
    return final_list


def gather_monthly_noaa_files(dir, noaa_station, ext, tyear, tmonth):
    glob_filter = create_station_monthly_glob_filter(
        noaa_station, ext, tyear, tmonth)
    rdir = r'{}'.format(dir)
    glob_path = Path(rdir)
    file_list = [str(pp) for pp in glob_path.glob(str(glob_filter))]
    final_l = []
    for f in file_list:
        final_l.append(os.path.split(f)[1])
    final_l.sort()
    return final_l


def load_monthly_noaa_csv_files(dir, noaa_station, ext="csv", tyear=2021, tmonth=0):
    file_list = gather_monthly_noaa_files(
        dir, noaa_station, ext, tyear, tmonth)
    if dir == ".":
        t_dir = ""
        trace_print(4, "Using Current working direcotry")
    else:
        t_dir = dir
        trace_print(4, " loading from dir: ", t_dir)
    month_df = pd.DataFrame()
    if len(file_list) > 0:
        for f in file_list:
            if file_exclusion_filter(f):
                continue
            obs1 = read_weather_obs_csv(t_dir + f)
            trace_print(4, "loading:  ", f)
            month_df = pd.concat( [ month_df,obs1 ], ignore_index=True)
            # month_df = month_df.append(obs1, ignore_index=True)
    return month_df


def load_range_noaa_csv_files(dir, noaa_station, ext="csv", startdt=0, enddt=0):
    file_list = gather_any_noaa_files(
        dir, noaa_station, ext, startdt, enddt)
    month_df = pd.DataFrame()
    if dir == ".":
        t_dir = ""
        trace_print(4, "Using Current working direcotry")
    else:
        t_dir = dir
        trace_print(4, " loading from dir: ", t_dir)
    if len(file_list) > 0:
        for f in file_list:
            obs1 = read_weather_obs_csv(t_dir + f)
            trace_print(4, "loading:  ", f)
            month_df = pd.concat( [month_df, obs1], ignore_index=True)
            #month_df = month_df.append(obs1, ignore_index=True)
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


def hunt_for_noaa_files3(dir, station_glob_filter, ext):
    """
    pass in a glob filter - it can be the station name. or a sub-filter
    version #2 has to be used with a less greedy filter.
    also - doesn't matter if you transition a year/month
    have to glob for that up frong.
    """
    target_csv = ""
    station_file_list = get_noaa_any_files(dir, station_glob_filter, ext)
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
        # trace_print( 4, "file: ", f)
        if (f.find('latest') > 1) or (f.find('dupcheck') > 1):
            continue
        f_station, f_year, f_month, f_day, f_hour = f.split("_")
        m1 = int(f_month[1:3])
        d1 = int(f_day[1:3])
        h1 = int(f_day[1:3])
        # trace_print( 4, " "m1,d1,h1", str(m1), " ", str(d1), " ", str(h1))
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


def knots(mph):
    return float(mph * 0.868976)


def cardinal_points(dir):
    # returns -1 on failure
    points = {
        "N": 0,
        "NORTH": 0,
        "NE": 45,
        "NORTEAST": 45,
        "E": 90,
        "EAST": 90,
        "SE": 135,
        "SOUTHEAST": 135,
        "S": 180,
        "SOUTH": 180,
        "SW": 225,
        "SOUTHWEST": 225,
        "W": 270,
        "WEST": 270,
        "NW": 315,
        "NORTHWEST": 315
    }
    mydir = str(dir.upper())

    try:
        my_point = points[mydir]
    except:
        print(f"unknown cardinal point: {mydir}")
        my_point = -1

    return my_point


def wind_text(dir):
    points = {
        "N": 'North',
        "NORTH": 'North',
        0:  "North",
        "NORTHEAST": "NorthEast",
        "NE": "NorthEast",
        45:  "NorthEast",
        "E": "East",
        90: "East",
        "EAST": "East",
        "SE": "SouthEast",
        135: "SouthEast",
        "SOUTHEAST": "SouthEast",
        "S": "South",
        180: "South",
        "SOUTH": "South",
        "SW":  "SouthWest",
        225: "SouthWest",
        "SOUTHWEST": "SouthWest",
        "W": "West",
        270: "West",
        "WEST":  "West",
        "NW": "NorthWest",
        315: "NorthWest",
        "NORTHWEST":  "NorthWest"
    }
    mydir = str(dir.upper())

    try:
        my_text = points[mydir]
    except:
        print(f"unknown text direction: {mydir}")
        my_text = -1

    return my_text


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

   # obs2 = load_monthly_noaa_csv_files(".", "KDCA", "csv", 2021, 4)

    # print(obs2.shape)

    # print(obs2.head(150))

    # obs2.to_csv("month_test.csv", index=False)

    T1 = datetime.datetime(2021, 1, 12)
    T2 = datetime.datetime(2021, 6, 14)

    print("diff_month: ", diff_month(T1, T2))

    print(gather_any_noaa_files(".", "KDCA", "csv", T1, T2))

    obs3 = load_range_noaa_csv_files(".", "KDCA", "csv", T1, T2)

    print(obs3.shape)

    print(obs3.head(150))

    print("testing knots")

    print(f"10 mph is { knots(10)} knots")

    print("testing cardinal points")

    test_pnts = ["South", "NW", "Southwest", "bat"]
    for pnt in test_pnts:
        my_pnt = cardinal_points(pnt)
        print(f"{pnt} is { my_pnt } degrees")

    test_dir = ["S", "SW", "N", "NW", "NE", "E"]

    for tdir in test_dir:
        my_cardinal_dir = wind_text(tdir)
        print(f'The text for {tdir} is {my_cardinal_dir} ')
