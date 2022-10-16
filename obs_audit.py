#!/usr/bin/env python3
import os
import argparse
from datetime import date, datetime, timedelta
from dateutil import parser
from dateutil.parser import ParserError
import calendar

from pandas import Timestamp
from obs_utils import *
from weather_obs import create_station_file_name
from obs_csv import obsCsvSetting,test_date_string
import pprint
from collections import Counter
from email.utils import format_datetime, parsedate_tz
from obs_time import *

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
        self.start_time = datetime.now()
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

    def process_missing_row( self, obs1, date_row_missing):
        #append at end and then sort by date
        new_data = obs1.astype(obs1.dtypes.to_dict())
        new_data = new_data.iloc[0:1,:]
        for col in new_data.columns:
            if new_data[col].dtype ==  np.float64:
                new_data[col].values[:] = np.NAN

        # print(new_data)
        #new_data = pd.DataFrame(obs1[-1:].values, columns=obs1.columns)
        #new_data = obs1.astype(obs1.dtypes.to_dict())
        new_data = new_data.reset_index( drop=True)
        row = 0
        print(f"date_row_missing: {date_row_missing}")
        new_dt = ObsDate( date_row_missing)
        new_dt.emit_type('reg')
        new_data.at[row, 'observation_time' ]  = pd.Timestamp(new_dt.get_datetime())
        new_dt.emit_type('rfc')
        new_data.at[row, 'observation_time_rfc822'] = new_dt
        new_data.at[row, 'wind_dir'] = np.nan
        new_data.at[row, 'wind_mph'] = np.nan 
        new_data.at[row, 'wind_gust_mph'] = np.nan
        new_data.at[row, 'temp_f'] = np.nan
        obs1 = obs1.append(new_data)
        # new_df = new_df.sort_values(by='observation_time')
        # new_df = new_df.reset_index( drop=True)
        print("tail")
        print(obs1.iloc[-5:, 10:])
        return obs1
    def process_missing_list( self, obs1, missing_dates):
        # index = missing_dates.pop()
        print(f"len:  {len(missing_dates)}")
        for dt1 in missing_dates:
            print(f'list process')
            if isinstance(dt1,Timestamp):
                obs1 = self.process_missing_row( obs1, dt1) 
        # missing_dates.append(index)    
        obs1 = obs1.sort_values(by='observation_time')
        obs1 = obs1.reset_index( drop=True)
        return obs1
        
    def process_incorrect_row( self, obs1, row, correct_date):
        print(f'correct_date: {correct_date}')
        print(f'row: { row } ')
        obs1.at[row, 'observation_time' ]  = correct_date
        obs1.at[row, 'observation_time_rfc822'] = format_datetime(correct_date)
        obs1.at[row, 'wind_dir'] = np.nan
        obs1.at[row, 'wind_mph'] = np.nan 
        obs1.at[row, 'wind_gust_mph'] = np.nan
        obs1.at[row, 'temp_f'] = np.nan
        return obs1
                
    def process_audit( self, obs1):
        prior = obs1['observation_time'][0]
        if prior.hour > 0:
            prior = prior.replace( hour = 0)
            print(f'prior after replace: {prior}')
        diff_time = 0
        date_list = []
        self.missing_time = {}
        for index, row in obs1.iterrows():
            curr_time = row['observation_time']
            # print( f'year: {curr_time.year} month: {curr_time.month} day: {curr_time.day}')
            print(f"processing row: { curr_time}")
            date_t = (curr_time.year, curr_time.month, curr_time.day)
            date_list.append(date_t)
            diff_time = curr_time - prior
            diff_time_hours = ((diff_time.total_seconds()/60)/60)
            if diff_time_hours != 1.0 and diff_time_hours != 0.0:   
                print(f"index: {index} row: {row['observation_time']}  diff:  {diff_time_hours}")
                if date_t not in self.missing_time.keys():
                    self.missing_time[date_t] = []
                if ( diff_time_hours > 0 ):
                    for i in range(int(diff_time_hours -1 )):
                        m_time = prior + timedelta( hours = (i + 1))
                        print(f"time missing: { m_time}")
                        self.missing_time[date_t].append(m_time)
                    self.missing_time[date_t].append(index)                  
            prior = row['observation_time']
        date_c = Counter( date_list)
        problem_dates = self.missing_time.keys()
        print(self.missing_time)
        # do incorrect dates first.
        # index will match up and just use at
        for date_e in date_c.keys():
             if date_c[date_e] == 24:
                print("process incorrect dates")
                
                if date_e in problem_dates:
                    print(f'procesing incorrects date: {date_e[1]}/{date_e[2]}/{date_e[0]} ')
                    idx = self.missing_time[date_e].pop()
                    new_dt = self.missing_time[date_e].pop()
                    obs1 = self.process_incorrect_row(obs1, idx-1, new_dt )
                    print( obs1.iloc[106:140,8:12])
        # add missing rows.
        # will be reindiexed and sorted.     
        for date_e in date_c.keys():
            print( "process missing dates")
            if date_c[date_e] < 24:    
                print(f'date: {date_e[1]}/{date_e[2]}/{date_e[0]}  count:  {date_c[date_e]} is less than 24 samples')
                obs1 = self.process_missing_list(obs1, self.missing_time[date_e])
                print( obs1.iloc[106:140,8:12])
     
        print(self.missing_time)
        print(date_c)
        return obs1
    def run(self):
        self.set_arg_parser()
        self.set_app_arguments()
        print(" running... ")
        obs1 = load_range_noaa_csv_files(self.app_setting.get_data_inputdir_path(),
                                         self.app_setting.station_prefix,
                                         "csv",
                                         self.app_setting.startdate,
                                         self.app_setting.enddate)
        print(f"number of observations for analysis: {obs1.shape[0]}")
        obs_audit = self.process_audit(obs1)
        obs_audit.to_csv("audit_01.csv")
        print(obs_audit.info())
        
        
        return
if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='obs_audit.py - %(message)s')
    logger = logging.getLogger('weather_obs_f')
    app = obsAuditAPP()
    app.run()