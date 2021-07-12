import os
import sys
import glob
import datetime
from pathlib import Path

import logging
logger = logging.getLogger('weather_obs_f')

def get_obs_csv_files(  dir ):
  glob_path = Path(dir)
  file_list = [str(pp) for pp in glob_path.glob("*.csv")]
  final_l = []
  for f in file_list:
      final_l.append( os.path.split( f )[1])
  final_l.sort()
  return final_l

def get_noaa_text_files(  dir, noaa_station ):
  glob_path = Path(dir)
  file_list = [str(pp) for pp in glob_path.glob(str(noaa_station+"*.txt"))]
  final_l = []
  for f in file_list:
      final_l.append( os.path.split( f )[1])
  final_l.sort()
  return final_l

def hunt_for_noaa_files(dir, station):
    station_file_list = get_noaa_text_files( dir, station)
    now = datetime.datetime.now()
    day = str(now.day)
    month = str(now.month)
    dirlist = os.listdir()
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
            if  d1 < int(day):
               logger.debug("In the past: %s" ,f)
    return target_csv

def construct_daily_cmd_call( file, obs_dir):
   myfile = file.split('.')
   c_file = " --file " +  file
   c_chart = " --chart " + myfile[0] + ".png"
   c_table = " --table " + myfile[0] + ".html"
   c_dir = " --dir " + obs_dir
   cmd = r"python " +  obs_dir + os.sep +  r"daily_weather_obs_chart.py" + c_file + c_chart + c_table + c_dir
   return cmd

if __name__ == "__main__":
  
   import logging
   logger = logging.getLogger('weather_obs_f')
   
   print("testing csv files")
   # change for unix
   obs_dir = r"C:\Users\jonallen\Documents\github\weather_obs"
   #obs_dir = r"/var/www/html/weather_obs"
   flist = get_obs_csv_files( obs_dir)

   print(flist)
   
   mycmd = construct_daily_cmd_call("KDCA_Y2021_M02_D19_H15.csv" , obs_dir)
   
   print("contruct command test:")
   print (mycmd)
   
   print("NOAA files")
   print("")
   
   print(get_noaa_text_files(".", "ANZ535"))
   
   print("hunting")
   print(hunt_for_noaa_files(".", "ANZ535"))
