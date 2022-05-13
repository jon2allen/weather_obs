#!/usr/bin/python3
import os
import argparse
from datetime import date, datetime, timedelta
from dateutil import parser
from dateutil.parser import ParserError
import calendar
from obs_utils import *
from weather_obs import create_station_file_name
from obs_csv import obsCsvSetting,test_date_string
import pprint
from collections import Counter

#
#  program to audit range
#  review for non-linear data - such as skipped hours, moving backwards, or no data
#  for a period of time.
#
class obsAuditSetting(obsCsvSetting):
    def __init__(self):
        print("audit settings")
        super().__init__()

class obsAuditAPP:
    def __init__(self):
        self.app_setting = obsAuditSetting()
        self.start_time = datetime.datetime.now()
        self.working_dir = os.getcwd()
        self.parser = argparse.ArgumentParser(description='obs_audit_utility')
    def set_arg_parser(self):
        self.parser.add_argument(
            "--inputdir", help='input direcotry for audit')
        self.parser.add_argument(
            '--startdate', help='startdate  YYYY/MM/DD ')
        self.parser.add_argument(
            "--enddate", help='enddate  YYYY/MM/DD ')
        self.parser.add_argument(
            "--station_prefix", help='4 character prefix \(ex. KDCA\)')
    def set_app_arguments(self):
        args = self.parser.parse_args()
        if (args.startdate):
            self.app_setting.startdate = args.startdate
        if (args.enddate):
            self.app_setting.enddate = args.enddate
        print(args.startdate)
        print(self.app_setting.startdate)
        print(args.enddate)
        if args.station_prefix:
            self.app_setting.station_prefix = args.station_prefix
        else:
            trace_print(4, " --station must be specified.")
            sys.exit(8)
    def process_audit( self, obs1):
        prior = obs1['observation_time'][0]
        print("prior: ", prior )
        diff_time = 0
        date_list = []
        for index, row in obs1.iterrows():
            curr_time = row['observation_time']
            # print( f'year: {curr_time.year} month: {curr_time.month} day: {curr_time.day}')
            date_t = (curr_time.year, curr_time.month, curr_time.day)
            date_list.append(date_t)
            diff_time = curr_time - prior
            diff_time_hours = ((diff_time.total_seconds()/60)/60)
            if diff_time_hours != 1.0 and diff_time_hours != 0.0:   
                print("index: " + str(index) + " row: " + str(row['observation_time']) + " diff: " + str(diff_time_hours))
            prior = row['observation_time']
        date_c = Counter( date_list)
        for date_e in date_c.keys():
            print(f'date: {date_e}  count:  {date_c[date_e]}')
    def run(self):
        self.set_arg_parser()
        self.set_app_arguments()
        print(" running... ")
        obs1 = load_range_noaa_csv_files(self.app_setting.get_data_inputdir_path(),
                                         self.app_setting.station_prefix,
                                         "csv",
                                         self.app_setting.startdate,
                                         self.app_setting.enddate)
        print(obs1.shape)
        self.process_audit(obs1)
        
        
        return
if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='obs_audit.py - %(message)s')
    logger = logging.getLogger('weather_obs_f')
    app = obsAuditAPP()
    app.run()