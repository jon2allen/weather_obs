#!/usr/bin/env python3
import os
import argparse
from datetime import date, datetime, timedelta
from dateutil import parser
from dateutil.parser import ParserError
import calendar
from obs_utils import *
from weather_obs import create_station_file_name
import pprint


def test_for_month(my_string):
    s = my_string.lower()
    for m in calendar.month_name:
        if m.lower() == s:
            return True
    for m in calendar.month_abbr:
        if m.lower() == s:
            return True
    return False


def test_date_string(date_str):
    try:
        obs_date = dateutil.parser.parse(date_str)
    except ParserError:
        obs_date = None
    return obs_date
#
# function takes create station name and stuff Range into it
# Might be an opportunity for a @decorator


def create_range_file_name(station='KDCA', ext='csv', obs_time_stamp=0, enddt=0):
    t_delta = ""
    if enddt != 0:
        t_delta = enddt.date() - obs_time_stamp.date()
    f_name = create_station_file_name(station, ext, obs_time_stamp)
    if (len(f_name) > 4):
        f_list = f_name.split('.')
        final_n = f_list[0] + "_Range" + str(t_delta.days) + "." + f_list[1]
        return final_n


def create_month_file_name(file_month, file_year, station='KDCA', ext='csv'):
    month_date_str = str(file_year) + "/" + str(file_month) + "/01"
    month_date = test_date_string(month_date_str)
    f_name = create_station_file_name(station, ext, month_date)
    if (len(f_name) > 4):
        f_list = f_name.split('.')
        final_n = f_list[0] + "_Monthly" + "." + f_list[1]
        return final_n


def create_year_file_name(file_year, station='KDCA', ext='csv'):
    year_date_str = str(file_year) + "/01/01"
    year_date = test_date_string(year_date_str)
    f_name = create_station_file_name(station, ext, year_date)
    if (len(f_name) > 4):
        f_list = f_name.split('.')
        final_n = f_list[0] + "_Yearly" + "." + f_list[1]
        return final_n
    
def write_obs_csv( f_name, obs1, force_flag):
    if os.path.exists(f_name):
        trace_print(4, "file exists - nothing written ")
        if force_flag:
           trace_print(4, "force flag set - overwriting file")
           obs1.to_csv(f_name, index=False, na_rep='<no_value_provided>') 
    else:
        obs1.to_csv(f_name, index=False, na_rep='<no_value_provided>')

# Class obsCsvSetting


class obsCsvSetting:
    def __init__(self):
        self.outdir = '.'
        self.inputdir = '.'
        # action can be "split or combine"
        self._action = ""
        # set month and year to now
        self._year = ""
        self._datestart = ""
        self._enddate = ""
        self.station_prefix = ""
        self._outfile = ""
        self._month = ""
        return

    def get_data_inputdir_path(self):
        s = os.sep
        # assume if has separtor that it is full path
        # if not - then it is relative to current workdir
        if (self.inputdir.find(s) >= 0):
            return self.inputdir + s
        if self.inputdir != '.':
            r_data_path = os.path.join(
                os.getcwd() + s + self.inputdir + s)
        else:
            r_data_path = '.'
        return r_data_path

    def get_data_dir_path(self):
        # should be fully qualified from set below
        return self.outdir

    def set_data_dir(self, args_dir):
        s = os.sep
        if (args_dir.find(s) > 0):
            t_dir = args_dir
        else:
            t_dir = os.getcwd() + s + self.outdir
            self.outdir = t_dir
        if (os.path.exists(t_dir)):
            trace_print(4, "data dir exists: ", t_dir)
        else:
            trace_print(4, "Data dir does not exist")
            os.mkdir(t_dir)
            trace_print(4, " directory created:", t_dir)

    @property
    def action(self):
        return self._action

    @action.deleter
    def action(self):
        del self._action

    @action.setter
    def action(self, csv_action):
        # if type(csv_year) is int:
        self._action = ""
        if (csv_action.lower() == "combine"):
            self._action = "combine"
        if (csv_action.lower() == "split"):
            self._action = "split"

    @property
    def startdate(self):
        return self._datestart

    @startdate.deleter
    def startdate(self):
        del self._datestart

    @startdate.setter
    def startdate(self, date_start):
        temp_date = test_date_string(date_start)
        if temp_date:
            self._datestart = temp_date
        else:
            self._datestart = ""

    @property
    def enddate(self):
        return self._enddate

    @enddate.deleter
    def enddate(self):
        del self._enddate

    @enddate.setter
    def enddate(self, date_end):
        temp_date = test_date_string(date_end)
        if temp_date:
            self._enddate = temp_date
        else:
            self._enddate = ""

    @property
    def outfile(self):
        return self._outfile

    @outfile.deleter
    def outfile(self):
        del self._oufile

    @outfile.setter
    def outfile(self, csv_file):
        self._outfile = csv_file

    @property
    def year(self):
        return self._year

    @year.deleter
    def year(self):
        del self._year

    @year.setter
    def year(self, csv_year=0):
        # if type(csv_year) is int:
        try:
            csv_year = int(csv_year)
        except ValueError:
            csv_year = None
            return
        if csv_year == 0:
            dt = datetime.date.now()
            self._year = dt.year
        if csv_year > 1900 and csv_year < 9999:
            self._year = csv_year

    @property
    def month(self):
        return self._month

    @month.deleter
    def month(self):
        del self._month

    @month.setter
    def month(self, csv_month=0):
        if csv_month.isdigit():
            csv_month = int(csv_month)
        if type(csv_month) is int:
            if csv_month == 0:
                dt = datetime.date.now()
                self._month = dt.month
            if csv_month > 0 and csv_month < 12:
                self._month = csv_month
            else:
                raise ValueError('invalid month: ', str(csv_month))
        if type(csv_month) is str:
            if test_for_month(csv_month):
                self._month = month_to_num(csv_month.lower())
            else:
                raise ValueError('invalid month: ', str(csv_month))


# Class obsCsvSplit
# after init - call do_split - the rest are internal functions.

class obsCsvSplit:
    def __init__(self, obs_setting):
        self.obs_setting = obs_setting
        self.outfile = obs_setting.outfile
        # if outfile = blank.  create file for writing.
        self.station_id = obs_setting.station_prefix
        self.outdir = obs_setting.outdir
        print(self.obs_setting.infile)
        self.obs2 = read_weather_obs_csv(self.obs_setting.infile)
        trace_print(4, "analyzing input...")
        self.obs2['day_of_month'] = self.obs2['observation_time'].dt.day
        self.obs2['month_num'] = self.obs2['observation_time'].dt.month
        self.obs2['year_obs'] = self.obs2['observation_time'].dt.year
        trace_print(4, "list of stations: ", str(
            self.obs2['station_id'].unique()))
        trace_print(4, "list of days: ", str(
            self.obs2['day_of_month'].unique()))
        trace_print(4, "list of months: ", str(
            self.obs2['month_num'].unique()))
        trace_print(4, "list of years: ", str(self.obs2['year_obs'].unique()))
        #self.split_each_year(self.obs_setting.station_prefix, self.obs2)

    def get_full_path_filename(self):
        #trace_print( 1, "outdir")
        #trace_print(1, self.obs_setting.get_data_dir_path())
        f_path = self.obs_setting.get_data_dir_path()
        if f_path == '.':
            return ""
        else:
            return f_path

    def create_station_name_from_date(self, station, year, month, day):
        ext = "csv"
        file_n = station + '_Y' + str(year) + '_M' + \
            str(month).zfill(2) + '_D' + str(day).zfill(2) + '_H00' + "." + ext
        return file_n

    def split_each_day(self, station, year, month, obs_month):
        num_days = obs_month['day_of_month'].unique()
        for day in num_days:
            # todo - filter by station
            obs3 = obs_month.loc[obs_month['day_of_month'] == day]
            trace_print(4, "day ", str(day), "  shape ", str(obs3.shape))
            f_name = self.create_station_name_from_date(
                station, year, month, day)
            f_path = self.get_full_path_filename()
            trace_print(4, f_path + os.sep + f_name)
            write_obs_csv(f_path + os.sep + f_name, obs3, self.obs_setting.force)
            #obs3.to_csv((f_path + os.sep + f_name), index=False,
            #            na_rep='<no_value_provided>')

    def split_each_month(self, station, year, obs2):
        obs_temp_month = obs2.loc[(obs2['year_obs'] == year)]
        months = obs_temp_month['month_num'].unique()
        for m in months:
            self.split_each_day(station, year, m, obs_temp_month)

    def split_each_year(self, station, obs2):
        years = obs2['year_obs'].unique()
        for year in years:
            self.split_each_month(station, year, obs2)

    def split_each_station(self, station, obs2):
        stations = obs2['station_id'].unique()
        # split each day/each station
        if station == '*':
            for st in stations:
                self.split_each_year(st, obs2)
        else:
            # only the specified station.
            obs_temp_station = obs2.loc[(obs2['station_id'] == station)]
            self.split_each_year(station, obs_temp_station)

    def do_split(self):
        self.split_each_station(self.obs_setting.station_prefix, self.obs2)
        #self.split_each_year( self.obs_setting.station_prefix, self.obs2)

# Class obsCsvCombine


class obsCsvCombine:
    def __init__(self, obs_setting):
        self.obs_set = obs_setting
        self.outfile = obs_setting.outfile
        # if outfile = blank.  create file for writing.
        self.station_id = obs_setting.station_prefix
        self.outdir = obs_setting.outdir

    def combine_monthly(self):
        obs1 = load_monthly_noaa_csv_files(self.obs_set.get_data_inputdir_path(),
                                           self.station_id,
                                           "csv",
                                           self.obs_set.year,
                                           self.obs_set.month)
        self.write_combine_csv(obs1)
        return

    def combine_range(self):
        obs1 = load_range_noaa_csv_files(self.obs_set.get_data_inputdir_path(),
                                         self.station_id,
                                         "csv",
                                         self.obs_set.startdate,
                                         self.obs_set.enddate)
        self.write_combine_csv(obs1)
    
    def combine_yearly(self):
        pass

    def write_combine_csv(self, obs1):
        if obs1.empty:
            trace_print(
                4, "Unable to find or load data - not writing CSV file")
        else:
            trace_print(4, "Writing out CSV: ", self.outfile)
            write_obs_csv(self.outfile, obs1, self.obs_set.force)


# Class obsCsvApp
# implement run here.

class obsCsvAPP:
    def __init__(self):
        self.app_setting = obsCsvSetting()
        self.start_time = datetime.datetime.now()
        self.working_dir = os.getcwd()
        self.parser = argparse.ArgumentParser(description='obs_csv_utility')

    def set_arg_parser(self):
        self.parser.add_argument('--combine', action="store_true",
                                 help='combine weather_obs CSV function ')
        self.parser.add_argument('--split', action="store_true",
                                 help='split weather_obs CSV function - splits into daily file ')
        self.parser.add_argument("--infile", help='in file for split')
        self.parser.add_argument(
            "--inputdir", help='input direcotry for combine')
        self.parser.add_argument(
            '--startdate', help='startdate  YYYY/MM/DD - combine only')
        self.parser.add_argument(
            "--enddate", help='enddate  YYYY/MM/DD - combine only')
        self.parser.add_argument("--outfile", help='outfile for combine')
        self.parser.add_argument(
            "--station_prefix", help='4 character prefix \(ex. KDCA\)')
        self.parser.add_argument(
            "--outdir", help='alternate dir to write date - split escpecially')
        self.parser.add_argument(
            "--force", action="store_true",help='force overwrite of files - combine only')
        self.parser.add_argument(
            "--month", help='month of data to act on - combine only')
        self.parser.add_argument("--year", help='year of data to act on')

    def set_app_arguments(self):
        args = self.parser.parse_args()
        # check to see if year/month or range specified.
        rng_date = False
        myr_check = False
        month_check = False
        self.app_setting.force = False
        if args.combine:
            self.app_setting.action = "combine"
        if args.split:
            self.app_setting.action = "split"
        if args.month:
            self.app_setting.month = args.month
            month_check = True
        if args.year:
            self.app_setting.year = args.year
            if month_check == False:
                # no month specified - get entire year.
                self.app_setting.startdate = "01/01/" + str(args.year) 
                self.app_setting.enddate = "12/31/" + str(args.year) 
            myr_check = True
        if args.outdir:
            self.app_setting.outdir = args.outdir
            self.app_setting.set_data_dir(args.outdir)
        if args.infile:
            self.app_setting.infile = args.infile
        if args.inputdir:
            self.app_setting.inputdir = args.inputdir
        if args.startdate:
            self.app_setting.startdate = args.startdate
            rng_date = True
        if args.enddate:
            self.app_setting.enddate = args.enddate
            rng_date = True
        if args.station_prefix:
            self.app_setting.station_prefix = args.station_prefix
        else:
            trace_print(4, " --station must be specified.")
            sys.exit(8)
        if args.force:
            self.app_setting.force = True
        if args.outfile:
            self.app_setting.outfile = args.outfile

        print(self.parser.parse_args())

        if myr_check and rng_date:
            trace_print(
                4, " Either --Month/--Day or --startdate/--enddate - can't do both")
            sys.exit(8)
        self.app_setting.rng_date = rng_date
        self.app_setting.myr_check = myr_check
        self.app_setting.month_check = month_check
        return

    def create_outfile( self, station, ext, type):
        if len(self.app_setting.outfile) > 1:
            return 
        if type == "Month":
            self.app_setting.outfile = create_month_file_name(self.app_setting.month,
                                                              self.app_setting.year,
                                                              self.app_setting.station_prefix,
                                                              'csv')
        if type == "Year":
            self.app_setting.outfile = create_year_file_name(self.app_setting.year,
                                                             self.app_setting.station_prefix,
                                                             'csv')
        if type == "Range":
            self.app_setting.outfile = create_range_file_name(self.app_setting.station_prefix,
                                                                'csv',
                                                                self.app_setting.startdate,
                                                                self.app_setting.enddate) 
            print("outfile: ", self.app_setting.outfile)           
        return
    def run(self):
        self.set_arg_parser()
        self.set_app_arguments()
        if self.app_setting.action == "combine":
            trace_print(4, " combine function")
            if self.app_setting.rng_date:
                self.create_outfile( self.app_setting.station_prefix, 'csv', "Range")
                cb1 = obsCsvCombine(self.app_setting)
                cb1.combine_range()
            elif self.app_setting.month_check == False:
                self.create_outfile( self.app_setting.station_prefix, 'csv', "Year")
                cb1 = obsCsvCombine(self.app_setting) 
                cb1.combine_range()         
            elif self.app_setting.myr_check:
                self.create_outfile( self.app_setting.station_prefix, 'csv', "Month")
                trace_print(4, "month: ", str(self.app_setting.month))
                cb1 = obsCsvCombine(self.app_setting)
                cb1.combine_monthly()
            else:
                trace_print(4, "unable to determine range, month or year")
            pass
        elif self.app_setting.action == "split":
            sp1 = obsCsvSplit(self.app_setting)
            sp1.do_split()
            pass
        return


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='obs_csv.py - %(message)s')
    logger = logging.getLogger('weather_obs_f')
    app = obsCsvAPP()
    app.run()

    # obs_csv.py --combine --startdate 05/01/2021 --enddate 05/29/2021 ---outfile KDCA_may_2021.csv


# monthly file is KDCA_Y2021_M05_D00_H00.csv    ( notice day is invalid 00)

# a range file.   so say April 10th to May 15th.

#  KDCA_Y2021_M04_D10_H00_range.csv   - so it is the start date of range plus "_range"
#  also means that you will have to load and search to the end to figure out range.
#   could add in number
#   perhaps KDCA_Y2021_M04_D10_H00_R40.csv   - R40 means 40 days.  always days.

# --daterange "04/15/2021 to 05/05/2021"


def testing1():
    m1 = "March"
    m2 = "Mazda"
    m3 = "Apr"

    print(test_for_month(m1))
    print(test_for_month(m2))
    print(test_for_month(m3))

    obs1 = obsCsvSetting()

    obs1.month = "March"

    print(obs1.month)

    try:
        obs1.month = "MAZDA"
    except ValueError:
        trace_print(4, "not a valid month")

    print(obs1.month)

    obs1.month = 4

    print(obs1.month)

    # obs1.month = 15

    obs1.startdate = "2021/03/02"

    print(obs1.startdate)

    obs1.enddate = " December 1st, 2021"

    print(obs1.enddate)

    obs1.year = 2021

    print(obs1.year)

    obs1.year = "2022"

    print(obs1.year)

    obs1.year = "bob u uncle"

    # testing1()
