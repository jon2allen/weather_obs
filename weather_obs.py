#!/usr/bin/env python3
"""
 Program:  weather_obs
 Author:   Jon Allen
 Purpose:  To take NOAA weather observations and place into CSV
 Command args:   
           init - create CSV file
           append - append reading to CSV file
           station -  NOAA observation URL
           collect -  run in backgroun and collect hourly, implied append
           cut   -  "weekly, daily, monthly"
                      Data per CSV file, default is daily.
           duration - "Always" ( means loop forever until killed. )
                    - 1 day, 1 week or 1 month.

  file format -  obs_<station>_year_month_day.csv
         exaample:  obs_KDCA_2021_02_01.csv
"""


import os
import sys
import argparse
import csv
import urllib.request
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timedelta
from dateutil import parser
from obs_time import ObsDate
import schedule
import hashlib
import inspect
import traceback
from obs_utils import hunt_for_noaa_csv_files, create_station_glob_filter
from obs_utils import trace_print, parse_date_from_station_csv
import logging
from logging.handlers import TimedRotatingFileHandler
"""
testing
"""
#from freezegun import freeze_time
#import unittest

#freezer = freeze_time("2021-12-31 23:56:30", tick=True)
#freezer.start()


logger = logging.getLogger('weather_obs_f')
csv_headers = ['credit', 'credit_URL', 'image', 'suggested_pickup', 'suggested_pickup_period', 'location', 'station_id', 'latitude', 'longitude', 'observation_time', 'observation_time_rfc822', 'weather', 'temperature_string', 'temp_f', 'temp_c', 'relative_humidity', 'wind_string', 'wind_dir', 'wind_degrees', 'wind_mph', 'wind_kt', 'wind_gust_mph',
               'wind_gust_kt', 'pressure_string', 'pressure_mb', 'pressure_in', 'dewpoint_string', 'dewpoint_f', 'dewpoint_c', 'heat_index_string', 'heat_index_f', 'heat_index_c', 'windchill_string', 'windchill_f', 'windchill_c', 'visibility_mi', 'icon_url_base', 'two_day_history_url', 'icon_url_name', 'ob_url', 'disclaimer_url', 'copyright_url', 'privacy_policy_url']
"""
   set the format from current date
   cannoical format for obs
"""


class ObsSetting:
    def __init__(self, station_url):
        self.set_station(station_url)
        # set defaults
        self.alt_processing = False
        self.ad_hoc = False
        self.use_alt_only = False
        self.run_minutes = 0
        self.dump_xml_flag = False
        self.init_csv = False
        self.cut_file = False
        self.append_data_specified = False
        self.append_data = False
        self.collect_data = False
        self.resume = False
        self.duration_interval = 0
        self.station_file = ""
        # schedule job
        self.job1 = None
        self.job2 = None
        self.obs_iteration = 0
        self.trace = True
        self.prior_obs_time = ""
        self.current_obs_time = ""
        self.data_dir = "."
        self.sched_interval = ":20"
        self.ignore_dup = False
        self.collection_date = False

    def __str__(self):
        my_str = "ObsSetting: "
        return my_str + str(self.primary_station) + " -> " + str(self.station_file)

    def __repr__(self):
        my_str = "ObsSetting: "
        return my_str + str(self.primary_station) + " -> " + str(self.station_file)

    def set_station(self, station_url):
        self.primary_station = station_url
        station = station_url[-8:]
        self.station_id = station[:4]
        self._trace("Station URL of XML - " + str(station_url))
        
    def set_alt_station(self, station_url):
        self.alt_processing = True
        self.alt_station = station_url
        station = station_url[-8:]
        self.alt_station_id = station[:4]
        self._trace("alt_station:   ", str(self.alt_station_id))
        self._trace("alt URL of alt XML - " + str(self.alt_station))

    def set_data_dir(self, args_dir):
        self.data_dir = args_dir
        t_dir = os.getcwd() + os.sep + self.data_dir
        self._trace("Data dir: " + str(t_dir))
        if (os.path.exists(t_dir)):
            self._trace("data dir exists: ", t_dir)
        else:
            self._trace("Data dir does not exist")
            os.mkdir(t_dir)
            self._trace(" directory created")

    def _trace(self,  s, *t1):
        jstr = ''.join(t1)
        msg1 = " " + s + jstr
        trace_print(4, self.station_id, msg1)

    def set_observation_times(self, prior, current):
        self.prior_obs_time = prior
        self.current_obs_time = current
        # TODO - maybe move to internal function that adds in self.station_id
        self._trace(" current_obs_time(driver):  ", str(self.current_obs_time))
        self._trace(" prior_obs_time(driver): ", str(self.prior_obs_time))

    def set_duration(self, duration):
        self.duration_interval = int(duration)
        trace_print(7, "duration interval: ", str(self.duration_interval))

    def set_xml_dump_flag(self, flag):
        self.dump_xml_flag = flag
        trace_print(7, "Dump xml flag: ", str(self.dump_xml_flag))

    def set_cut_process(self):
        self._trace("cut specified")
        self.cut_file = True
        self.append_data = False

    def set_append_processing(self):
        self._trace("append specified")
        self.append_data = True
        self.append_data_specified = True
      # collect asssumes append

    def set_resume_processing(self):
        self._trace("resume specified")
        self.cut_file = True
        self.append_data_specified = True
        self.append_data = True
        self.resume = True

    def set_init_processing(self, file):
        self.init_csv = True
        self.station_file = file
        self._trace("init_csv: file  ", self.station_file)
    
    def get_data_dir_path( self ):
        if self.data_dir != '.':
            s = os.sep
            r_data_path = os.path.join(
                os.getcwd() + s + self.data_dir + s )
        else:
            r_data_path = '.'         
        return r_data_path
        


def get_obs_time(obs_date):
    t_str = obs_date
    if (obs_time_debug):
        trace_print(4, "Local observation time ( get_obs_time): ", t_str)
        # actual timezone is not important for obs file output.
        # obs_date = datetime.strptime( t_str[:20], "%b %d %Y, %I:%M %p ")
        obs_date = ObsDate(t_str[:20])
        # obs_date = parser.parse(t_str[:20])
        # adjust stamp for specific test
        obs_date = obs_date + timedelta(hours=obs_debug_t_delta)
        trace_print(4, "Debug obs_date:", str(obs_date))
        return obs_date
    trace_print(4, "Local observation time ( get_obs_time): ", t_str)
    # actual timezone is not important for obs file output.
    obs_date = ObsDate(t_str[:20])
    #obs_date = parser.parse(t_str[:20])
    # obs_date = datetime.strptime( t_str[:20], "%b %d %Y, %I:%M %p ")
    trace_print(4, "get_obs_time return()")
    return obs_date


def create_station_file_name(station='KDCA', ext='csv', obs_time_stamp=0):
    """ 
    create station file from current time or time provided
    Year_Month_Day_Hour - smalles unit is hour. 
    """
    if (obs_time_stamp == 0):
        t_now =ObsDate.now()
    else:
        t_now = obs_time_stamp
    year, month, day, hour, min = map(
        str, t_now.strftime("%Y %m %d %H %M").split())
    file_n = station + '_Y' + year + '_M' + \
        month + '_D' + day + '_H' + hour + "." + ext
    return file_n


#def create_station_file_name2(station="https://w1.weather.gov/xml/current_obs/KDCA.xml", ext='csv'):
def create_station_file_name2(obs1, ext='csv'):
    """ 
    create_station_file from observation time 
    """
    w_xml = get_weather_from_NOAA(obs1.primary_station)
    if (obs_check_xml_data(w_xml) == False):
        return ""
    headers, row = get_data_from_NOAA_xml(w_xml)
    obs_date = get_obs_time(row[9])
    
    obs1.station_file_time = obs_date

    station_id = obs1.primary_station[-8:-4]
    year, month, day, hour, min, am = map(
        str, obs_date.strftime("%Y %m %d %H %M %p").split())
    file_n = station_id + '_Y' + year + '_M' + \
        month + '_D' + day + '_H' + hour + "." + ext
    trace_print(4, "file name2 : obs_date: ", str(obs_date))
    return file_n


"""
   Get arguments
"""


def weather_obs_init():
    """ init the app, get args and establish globals """
    parser = argparse.ArgumentParser(description='NOAA weather obsevation')
    parser.add_argument('--init', help='Initialize CSV')
    parser.add_argument('--station', help='URL of station')
    parser.add_argument('--station_id', help='station id')
    parser.add_argument('--alt_station_id', help='alternate source converted to xml')
    parser.add_argument('--alt_only', help='alt stationid only', action="store_true")
    parser.add_argument('--xmldata', help='location of file xmldata - for testing')
    parser.add_argument(
        '--collect', help='Run collectiion in background - Y/N', action="store_true")
    parser.add_argument('--append', help='Append data to CSV file - specifed')
    parser.add_argument('-d', '--duration',
                        help='Duration cycle - default - 24 hours ')
    parser.add_argument('-c', '--cut', action="store_true")
    parser.add_argument('-x', '--xml', action="store_true")
    parser.add_argument(
        '-r', '--resume', help='resume append and cut', action="store_true")
    parser.add_argument('-j', '--json', help="generate json data to file")
    parser.add_argument(
        '-f', '--file', help="read stations from file specified")
    parser.add_argument('--dir', help='data directory offet- default is cwd ')
    parser.add_argument('--interval', help='interval for append')
    parser.add_argument('--ignoredup', help='ignore duplicates', action="store_true")
    parser.add_argument('--nologging', help='no logging file', action="store_true")
    parser.add_argument('--collectdate', help='add a collection date at end of CSV row', action="store_true")
    args = parser.parse_args()
    # trace_print(1, "parsing args...")
    # cannocial header
    # can't depend on xml feed to complete every value
    global csv_headers
    
    def check_resume_file(obs_setting ):
        today = ObsDate.now()
        day_1 = timedelta(hours=24)
        tomorrow = today+ day_1
        trace_print(4, "station_id", obs_setting.station_id)    
        today_glob = create_station_glob_filter(
            obs_setting.station_id, "csv", today)
        # Guam or Hawaii might be actually ahead.
        tomorrow_glob = create_station_glob_filter(
            obs_setting.station_id, "csv", tomorrow)
        last_file = hunt_for_noaa_csv_files(obs_setting.data_dir, tomorrow_glob) 
        if  len(last_file ) < 1:
            last_file = hunt_for_noaa_csv_files(obs_setting.data_dir, today_glob)
        return last_file
    
    def check_params2(obs_setting, args):
        obs_setting.station_file = create_station_file_name2(
            obs_setting)
        trace_print(3, "station file time: ", str(obs_setting.station_file_time) )
        if(args.dir):
            obs_setting.set_data_dir(args.dir)
        #    obs_setting.data_dir = args.dir
        #    if (os.path.exists( os.getcwd() + os.sep + obs_setting.data_dir)):
        #        trace_print(4, "data dir exists: ", str(obs_setting.data_dir))
        #    else:
        #        trace_print(1, "Data dir does not exist")
        #        os.mkdir( os.getcwd() + os.sep + obs_setting.data_dir)
        #        trace_print(1, " directory created")
        if (obs_setting.append_data_specified == False):
            trace_print(4, "Station filename: ", obs_setting.station_file)
        obs_setting.init_csv = True
        # initialize a CSV until we prove we are appending.
        if (args.init):
            obs_setting.set_init_processing(args.init)
        if (obs_setting.append_data_specified == True):
            obs_setting.station_file = args.append
            obs_setting.init_csv = False
            if (obs_setting.resume == True):
                trace_print(4, "resume here")
                #now = ObsDate.now()()
                #file_id = obs_setting.station_id + "_Y" + str(now.year)
                # file_id = obs_setting.station_file
                # TODO - support yesterday, today, and tomorrow.
                # Guam is actually tomorrow in many cases
                # so resume will not work if just today and yesterday
                # 24 hours +/- otherwise just create a new file
                data_path = obs_setting.get_data_dir_path()
                trace_print(3, "data path: ", data_path)
                obs_setting.station_file = check_resume_file( obs_setting)
                if (len(obs_setting.station_file) < 4):
                    obs_setting.station_file = create_station_file_name2(
                        obs_setting)
                    obs_setting.init_csv = True
                    obs_setting.append_data = False
                    obs_setting.append_data_specified = True
                    trace_print(3, "Resume - No file file on current day")
                c_date = parse_date_from_station_csv(obs_setting.station_file)
                obs_setting.station_file_time = datetime(c_date.year, c_date.month, c_date.day, 0, 0)
                trace_print(3, "station file time: ", str(obs_setting.station_file_time))
                trace_print(3, "station_file: ", obs_setting.station_file)

            trace_print(4, "Station id ( append ): ", obs_setting.station_file)
        if (args.xml == True):
            obs_setting.set_xml_dump_flag(True)
        if (args.ignoredup == True):
            obs_setting.ignore_dup = True

        if (args.collectdate == True):
            obs_setting.collection_date = True
        if (args.collect):
            trace_print(4, "collect in station setting")
            obs_setting.collect_data = True
            if (obs_setting.init_csv == False) and (obs_setting.append_data_specified == False):
                obs_setting.station_file = create_station_file_name2(
                    obs_setting)
                trace_print(4, "Station filename (collect): ",
                            obs_setting.station_file)
        if (args.alt_station_id):
            obs_setting.set_alt_station(str(args.alt_station_id))
        if (args.alt_only):
            obs_setting.use_alt_only = True
            trace_print(4, "use alt only set")
        return True
    if (args.nologging == True):
            disable_logging_file()
    if (args.file):
        try:
            with open(args.file, "r") as obs_file1:
                obs_entry_list = obs_file1.readlines()
                trace_print(4, str(obs_entry_list))
        except:
            print("Unable to open: ", args.file)
            sys.exit(4)
        setting_list = []
        # entries must be on the first 47 lines - no more or less - discard \n or other stuff
        for entry in obs_entry_list:
            setting_list.append(ObsSetting(entry[0:47]))
        trace_print(4, str(setting_list))
        for entry in setting_list:
            check_parms1(entry, args)
            trace_print(4, "Station id:  ", entry.station_id)
            check_params2(entry, args)
        return setting_list
    # check station and fill out appropriate values
    if (args.station):
        obs_setting = ObsSetting(args.station)
        check_parms1(obs_setting, args)
        trace_print(4, "Station id:  ", obs_setting.station_id)
        check_params2(obs_setting, args)
    else:
        trace_print(3, "Error: No station given - please use --station")
        trace_print(3, " see readme")
        sys.exit(4)
    obs_setting_list = []
    obs_setting_list.append(obs_setting)
    return obs_setting_list


def check_parms1(obs_setting, args):
    """ check standalong parms """
    if (args.duration):
        obs_setting.duration_interval = int(args.duration)
        duration_interval = int(args.interval)
        trace_print(1, "duration interval: ", str(args.duration))
    if (args.cut):
        obs_setting.set_cut_process()
        trace_print(1, "cut specified")
    if (args.append):
        obs_setting.set_append_processing()
        trace_print(1, "append specified")
        # collect asssumes append
    if (args.resume):
        obs_setting.set_resume_processing()
        trace_print(1, "resume specified")
    return True




"""
 function: dump_xml
   purpose:  dump raw xml for debugging
    inputs:
      raw xmldata
      iteration of dump
 outputs:
      file:  xml_dump + <iteration> . xml
"""


def dump_xml(obs1, xmldata, iteration):
    """  dump the xml to a file for deugging """
    trace_print(1, "dumpxml_entry")
    if (obs1.dump_xml_flag == True):
        trace_print(1, "dump_xml")
        file = "xml_dump" + str(iteration) + ".xml"
        fh = open(file, 'wb')
        fh.write(xmldata)
        fh.close()


"""
 function: trace_print
    tracing to stdout with flag
 inputs:
      trace string + list of stuff that can 
      be converted into strings.
      debug = 1 - debug
      debug = 2 - critcal
      debug = 3 - warning
      debug = 4 - informational, normal confirmation noise.
      theory here is to set all new trace_print to 1
      then go back and set it up higher.  less changes.
 outputs:
      function trace string.
"""





"""
   function:  get_last_csv_row
        get last line of current csv
   inputs:  file name
   output:  list of last row
"""


def get_last_csv_row(st_file):
    """ helper function to get last row of csv file """
    row_count = 0
    try:
        with open(st_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                row_count +=1
            if row_count > 1:
                return row
            else:
                return " "
    except:
        trace_print(3, "csv file not found... continue...")
        return ""


def obs_check_xml_data(xmldata):
    if (len(xmldata) < 4):
        trace_print(4, "No XML data to process")
        return False
    else:
        return True


def obs_sanity_check(obs1,  xml_data, data_row):
    """ checks wind to see if value is present """
    # df[['observation_time','wind_mph','wind_dir','wind_string']]
    table_col_list = [9, 19, 17, 16]
    for col in table_col_list:
        if (data_row[col].startswith("<no") == True):
            now = ObsDate.now()
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            seconds = (now - midnight).seconds
            obs1.dump_xml_flag = True
            dump_xml(obs1, xml_data, seconds)
            obs1.dump_xml_flag = False
            trace_print(
                4, "potential bad xml - see xml dump at ", str(seconds))
            return False
        trace_print(1, "data checked: ", str(data_row[col]))
    return True


"""
   function: duplicate_observation
        test curret observation date against csv file last row
   inputs:  list of observations from parse of xml file
   output:  True = if duplicate, false if not or csv empty.
"""


def duplicate_observation(obs1, current_obs):
    """ test last line of csv for duplicate """
    """ finds observation times and compares"""
    if obs1.ignore_dup == True:
        trace_print(3, "Ignore dup" )
        return False  
    r_csv_file = get_obs_csv_path(obs1, obs1.station_file)
    last_one = get_last_csv_row(r_csv_file)
    if (len(last_one) < 4):
        return False
    last_obs_dt = last_one[10]
    # last_obs_dt = last_obs_dt[:-1]
    trace_print(1, "last_obs:", last_obs_dt, "len ", str(len(last_obs_dt)))
    trace_print(1, "current_obs: ", current_obs[6], "  ", current_obs[10], "len ", str(
        len(current_obs[10])))
    cur_obs = ObsDate( current_obs[10]).get_datetime()
    last_obs = ObsDate( last_obs_dt).get_datetime()
    trace_print(4, "curr_obs", str( cur_obs))
    trace_print(4, "last_obs", str(last_obs))
    if obs1.station_file_time.date() > cur_obs.date():
        trace_print(4, "current obs older than file date")
        return True
    if cur_obs == last_obs:
        trace_print(4, "Is equal by datetime")
        return True
    if cur_obs < last_obs:
        trace_print(4, "cur_obs before last_obs")
        return True
    if (current_obs[10] == last_obs_dt):
        trace_print(1, "Is equal")
        return True
    return False


"""
 function: get_weather_from_NOAA(xmldata)
 inputs:
      station - URL of station XML
 outputs:
      tuple ( header, data ) - for updating CSV file
      trace - print md5 to validate changes in xml data form NOAA
            - there are episodes where it doesn't change, etc.
"""


def get_weather_from_NOAA(station):
    """ simple get xml data, and print the md5 """
    trace_print(4, "url request")
    try:
        with urllib.request.urlopen(station) as response:
            xml = response.read()
        trace_print(4, "xml md5: ",  hashlib.md5(xml).hexdigest())
    except:
        trace_print(4, "URL request error")
        xml = ""
    return xml


"""
   function:  transforam observation
     - puts data into more usuable format for excel or pandas
   inputs:  xml attribute, data
   outputs:  data for writing out to CSV
"""


def transform_observation(attribute, data):
    """ place to hook any transforms on te data """
    output = data
    if (attribute == 'observation_time'):
        output = data[16:]
    return output


"""
 function: get_data_from_NOAA_xml(xmldata)
 inputs:
      xmldata - string with XML data from NOAA obs
 outputs:
      tuple ( header, data ) - for updating CSV file

 Note:
 it appears that windchill isn't always included
 how to handle - exclude windchill
 explictily add it in. ??
 also visibility is a problem - not aways there.
 maybe just pre-populate is the best answer.
 if you have no windchill - it will not be there 
 heat index also the same - only include in 
 warmer times??
 later. Examination of data online - appears the case
 Decided to use fixed header list
 match up headers with xml incoming
 set data to <no_value_provided>
 transform data before adding to the writing list of rows
"""


def get_data_from_NOAA_xml(xmldata):
    """ parse noaa observatin data from xml into list """
    tree = ET.fromstring(xmldata)
    h1 = []
    r1 = []
    r1_final = []
    global csv_headers
    trace_print(4, "parsing NOAA xml")
    for child in tree:
        h1.append(child.tag)
        r1.append(child.text)
    for ch in csv_headers:
        if not r1:
            r1_final.append('')
        elif (ch in h1):
            r1_final.append(transform_observation(ch, r1.pop(0)))
        else:
            r1_final.append('<no_value_provided>')
    h1 = csv_headers
    return h1, r1_final

def add_collection_time( w_header, w_row):
    """
       add collection entries at end of each row
    """
    w_header.append("collection time")
    dt1 = ObsDate(datetime.now())
    dt1.emit_type("reg")
    w_row.append(str(dt1)) 
    return w_header, w_row
"""
 function: weather_csv_driver
 inputs:
      mode = "w" for write or "a" for append
      csv_file = fully formed file name
      w_header, w_row  = weather xml data from NOAA in
                 in binary string - broken into lists
"""


def weather_csv_driver(obs1, mode, csv_file, w_header, w_row):
    """ write out csv data - mode is append, write or cut """
    cut_mode = False
    trace_print(4, 'csv_driver')
#   if ( mode != 'w' ) and  ( mode != 'a' ):
#     trace_print( 1, " mode is invalid")
#     return False
    if (len(csv_file) < 4):
        print("CSV file must contain station name")
        return False
    # add colleciton time.
    if obs1.collection_date == True:
        trace_print(3, "collection date added")
        w_header, w_row = add_collection_time( w_header, w_row)
    if (mode == 'c'):
        # cut file request is active
        # denote the special mode and change it to write.
        cut_mode = True
        mode = 'w'
    r_csv_file = get_obs_csv_path(obs1, csv_file)
    trace_print(4, "data_dir location: ", str(r_csv_file))
    # newline parm so that excel in windows doesn't have blank line in csv
    # https://stackoverflow.com/questions/3348460/csv-file-written-with-python-has-blank-lines-between-each-row
    with open(r_csv_file, mode, newline='') as weather_file:
        weather_writer = csv.writer(
            weather_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        if (mode == 'w'):
            trace_print(4, "csv_driver: header")
            weather_writer.writerow(w_header)
            if (cut_mode == False):
                trace_print(4, "csv_driver: row_with_header")
                weather_writer.writerow(w_row)
        elif (mode == 'a'):
            trace_print(4, "csv_drver: row_only")
            weather_writer.writerow(w_row)
    # do I really need close??? with does this
    weather_file.close()
    csv_write_time = ObsDate.now()
    trace_print(4, "csv_write_time: ",
                csv_write_time.strftime("%A, %d. %B %Y %I:%M%p"))
    return True


def get_obs_csv_path(obs1, csv_file):
    if obs1.data_dir != '.':
        s = os.sep
        r_csv_file = os.path.join(
            os.getcwd() + s + obs1.data_dir + s + csv_file)
    else:
        r_csv_file = csv_file
    return r_csv_file


def weather_collect_ad_hoc( obs1 ) :
    trace_print(4, "collect _ad_hoc")
    # swap alt and primary?
    temp_station = obs1.primary_station
    trace_print(4, "primary station: ", str(temp_station))
    try:
        obs1.primary_station = obs1.alt_station
        trace_print(4, "alt station used:", str(obs1.primary_station))
    except:
        trace_print(4, "no alt station set")
        return
    obs1.ad_hoc = True
    # set a flag instead another parm )
    if weather_collect_driver(obs1):
        trace_print(4, "ad hoc No duplicate")
    else:
        trace_print(4, "ad hoc duplicate")
    obs1.primary_station = temp_station
    obs1.ad_hoc = False
    # schedule.cancel_job(obs1.job2)
    trace_print(4, "exit ad hoc")
    return schedule.CancelJob

    

"""
function: weather_collect_driver
 inputs:
      xml_url = URL to station XML
      csv_out = output file
 outputs:  returns TRUE
      Appends ( only ) csv file with data from obs xml
"""


def weather_collect_driver(obs1):
    """ Appends ( only ) csv file with data from obs xml """
    trace_print(4, "weather_collect_driver")
    xmldata = get_weather_from_NOAA(obs1.primary_station)
    if (obs_check_xml_data(xmldata) == False):
        return False
    outdata = get_data_from_NOAA_xml(xmldata)
    # check data and dump xml for post-mortem
    # data feed from noaa has unexpected output
    # check to see if wind is missing.
    obs_sanity_check(obs1, xmldata, outdata[1])
    # use for cut logic.
    # if local time crossed midnight - cut a new file.
    # save prior - obs_time_prior
    # curent to - obs_time_curent.
    trace_print(4, "station_id: ", str(obs1.station_id))
    trace_print(4, "current_obs_time(driver_before):  ",
                str(obs1.current_obs_time))
    trace_print(4, "prior_obs_time(driver_before): ", str(obs1.prior_obs_time))
    # if it comes in at zero hour ( mindnight) then reset current and prior
    obs1.prior_obs_time = obs1.current_obs_time
    obs1.current_obs_time = get_obs_time(outdata[1][9])
    obs1.local_time = ObsDate( outdata[1][9])
    obs1.current_local_time = obs1.local_time.local_now_reg()
    trace_print(4, "Local time now: ", str(obs1.current_local_time)) 
    if obs1.current_obs_time.day != obs1.current_local_time.day:
        trace_print(4, "obs time not on same day, exiting... ", str(obs1.current_obs_time))
        obs_schedule_alt_jobs(obs1)
        return False
    if (obs1.prior_obs_time.hour == 23):
        trace_print(4, "Special driver processing at hour 23")
        obs1.prior_obs_time = obs1.current_obs_time
    trace_print(4, "current_obs_time(driver):  ", str(obs1.current_obs_time))
    trace_print(4, "prior_obs_time(driver): ", str(obs1.prior_obs_time))
    if (duplicate_observation(obs1, outdata[1])):
        trace_print(3, " duplicate in collect.  exiting...")
        if (obs1.alt_processing == True and obs1.ad_hoc == False):
            obs_schedule_alt_jobs(obs1)
        return False
    weather_csv_driver(obs1, 'a', obs1.station_file, outdata[0], outdata[1])

    obs1.obs_iteration = obs1.obs_iteration + 1
    dump_xml(obs1, xmldata, obs1.obs_iteration)
    return True

def obs_schedule_alt_jobs(obs1):
    trace_print(4, "scheduling ad_hoc @:41")
    obs1.job2 = schedule.every().hour.at(":41").do(weather_collect_ad_hoc, obs1)
    trace_print(4, "Alt schedule job @ ", str(obs1.alt_station),
                " -> ", str(obs1.station_file))
    if obs1.current_local_time.hour < 4:
        obs1.job3 = schedule.every().hour.at(":24").do(weather_collect_ad_hoc, obs1)
        trace_print(4, "Alt schedule at :24 job @ ", str(obs1.alt_station),
                            " -> ", str(obs1.station_file))

def weather_obs_app_start(obs1):
    """ top level start of collection """
    # if appending and scheduling - skip over to collect
    if obs1.use_alt_only == True:
        trace_print(4, "setting station id to alt")
        obs1.primary_station = obs1.alt_station 
    trace_print(3,"weather_obs_app_starT() enter ")
    if (obs1.append_data != True):
        content = get_weather_from_NOAA(obs1.primary_station)
        if (obs_check_xml_data(content) == False):
            return False
        xmld1 = get_data_from_NOAA_xml(content)
        obs_string = xmld1[1][9]
        trace_print(4, "raw observation string: ", obs_string)
        obs_time_stamp = get_obs_time(obs_string)
        obs1.prior_obs_time = obs_time_stamp
        obs1.current_obs_time = obs_time_stamp
        trace_print(4, "current_obs_time(start):  ",
                    str(obs1.current_obs_time))
        trace_print(4, "prior_obs_time:(start) ", str(obs1.prior_obs_time))
        weather_csv_driver(obs1, 'w', obs1.station_file, xmld1[0], xmld1[1])
        trace_print(4, "Initializing new file (app_start): ",
                    str(obs1.station_file))
        dump_xml(obs1,  content, ObsDate.now().minute)
    if (obs1.collect_data == True):
        if obs1.job1:
            trace_print(4, "schedule job set - exit()")
            return
       
        obs1.append_data = True
        if obs1.use_alt_only == True:
             obs1.primary_station = obs1.alt_station
             trace_print(4, "alt only....")
             obs1.job1 = schedule.every().hour.at(obs1.sched_interval).do(weather_collect_ad_hoc, obs1)
        else:
             trace_print(4, "regular")
             obs1.job1 = schedule.every().hour.at(obs1.sched_interval).do(weather_collect_driver, obs1)
        trace_print(4, "schedule job @ ", str(obs1.primary_station),
                    " -> ", str(obs1.station_file))
        # execute every 20 minutes.   
    return
#
#
# need to specify file.


def weather_obs_app_append(obs1):
    """ append top level """
    content = get_weather_from_NOAA(obs1.primary_station)
    if (obs_check_xml_data(content) == False):
        return False
    xmld1 = get_data_from_NOAA_xml(content)
    dump_xml(obs1, content, ObsDate.now().minute)

    """
    test if last row and what is coming in are equal
  """
    # if --resume is specified - then we need to set prior to current.
    try:
        obs1.prior_obs_time = obs1.current_obs_time
    except:
        obs1.prior_obs_time = get_obs_time(xmld1[1][9])
    obs1.current_obs_time = get_obs_time(xmld1[1][9])
    trace_print(4, "current_obs_time(append):  ", str(obs1.current_obs_time))
    trace_print(4, "prior_obs_time(append): ", str(obs1.prior_obs_time))
    if (duplicate_observation(obs1, xmld1[1])):
        trace_print(3, 'duplicate append, exit up')
        # error on double start
        obs1.prior_obs_time = obs1.current_obs_time
        return
    weather_csv_driver(obs1, 'a', obs1.station_file, xmld1[0], xmld1[1])
    return
#


def duration_cut_check2(obs1, t_last, t_curr, hour_cycle):
    """ see if new file is to be created or cut """
    trace_print(1, "Duration check 2")
    t_now = t_curr
    trace_print(1, "obseration: ", str(obs1.station_id))
    trace_print(1, "t_now: ", str(t_now))
    trace_print(1, "t_last: ", str(t_last))
    if t_now.year > t_last.year:
        trace_print(1, "Duration year check")
        return True
    if t_now.month > t_last.month:
        trace_print(1, "Duration month check")
        return True
    if t_now.day > t_last.day:
        trace_print(1, "Duration day check")
        return True
    if (t_now.hour - t_last.hour == 0):
        return False
    if (hour_cycle > 0):
        if ((t_now.hour - t_last.hour) % hour_cycle == 0):
            trace_print(1, "Duration cycle check at ", str(hour_cycle))
            return True
    return False


"""
  Pass last cut time and check against now
  check at each day, month, and year.
  hour cycle is every so many hours
  No need to support less - observations are hourly
  """


def duration_cut_check(t_last, hour_cycle):
    """ see if new file is to be created or cut """
    trace_print(1, "Duration check")
    t_now = ObsDate.now()
    if t_now.year > t_last.year:
        trace_print(1, "Duration year check")
        return True
    if t_now.month > t_last.month:
        trace_print(1, "Duration month check")
        return True
    if t_now.day > t_last.day:
        trace_print(1, "Duration day check")
        return True
    if (t_now.hour - t_last.hour == 0):
        return False
    if (hour_cycle > 0):
        if ((t_now.hour - t_last.hour) % hour_cycle == 0):
            trace_print(1, "Duration cycle check at ", str(hour_cycle))
            return True
    return False


def foreach_obs(function, obs_list):
    """ foreach loop """
    for obs in obs_list:
        function(obs)
        
def obs_already_cut_today( station_file_name, current_time):
    file_dt = parse_date_from_station_csv( station_file_name)
    if file_dt.day == current_time.day:
        return True
    else:
        return False


def obs_cut_csv_file(obs1):
    """ determines if day transition has happened and starts a new CSV """
    if (obs1.cut_file == True):
        # obs_cut_time = obs1.current_obs_time + timedelta(minutes=10)
        obs1.local_time = obs1.current_obs_time
        obs1.current_local_time = obs1.local_time.local_now_reg()
        obs_cut_time = obs1.current_local_time
        # cut time should be 10 minutes ahead 
        # NOAA observations at at approx 50 minutes after the hour
        if (duration_cut_check2(obs1, obs1.prior_obs_time, obs_cut_time, obs1.duration_interval)):
            run_cut_operation(obs1, obs_cut_time)

def run_cut_operation(obs1, obs_cut_time):
    trace_print(4, "running cut operation")
    if obs_already_cut_today(obs1.station_file, obs_cut_time):
        trace_print(4, "already cut - return")
        return
            # sychronize obs_time for new day - so file name will be corrrect
            # last observation at 11:50 or so - add 10 minutes for file create.
    obs1.station_file = create_station_file_name(
                obs1.station_id, "csv", obs_cut_time)
            # start a new day cycle
    obs1.station_file_time = obs_cut_time
    obs1.prior_obs_time = obs_cut_time
    obs1.current_obs_time = obs_cut_time
    trace_print(4, "New Station file (cut):", obs1.station_file)
            # create new file with cannocial headers
    weather_csv_driver(obs1, 'c', obs1.station_file, csv_headers, [])
    schedule.cancel_job(obs1.job1)
            # we rassigned the next station file
            # new writes should go there.
    obs1.job1 = None
    t_begin = ObsDate.now()
    trace_print(4, "Time of last cut:",
                        t_begin.strftime("%A, %d. %B %Y %I:%M%p"))
            # this will reschedule job with new file.
    weather_obs_app_start(obs1)


def main_obs_loop(obs1_list):
    """ main loop - runs schedule and test for cut csv condition """
    run_minutes = ObsDate.now().minute
    if ((run_minutes == 59)):
        # every hour check to see if need to cut
        trace_print(1, "Num minutes running: ", str(run_minutes))
        foreach_obs(obs_cut_csv_file, obs1_list)
    else:
        trace_print(1, "run pending")
        schedule.run_pending()
        # schedule.run_all()
        # print out jobs every 5 minutes
        if run_minutes % 5 == 0:
            all_jobs = schedule.get_jobs()
            for j in all_jobs:
                trace_print(1,  str(j))
    time.sleep(60)


def weather_obs_app():
    obs1_list = weather_obs_init()
    # currently all options are same as first entry
    obs1 = obs1_list[0]
    if (obs1.init_csv == True):
        trace_print(4, "Init... ")
        foreach_obs(weather_obs_app_start, obs1_list)
    if (obs1.append_data_specified == True):
        if (obs1.resume == True):
            trace_print(1, "resume - with append")
        trace_print(1, "Appending data")
        # resume sets init_csv - have to retest again
        # resume sets thsi when a new file has to be created
        # resume starts next day.
        # try to resume same day - if not start a new day csv
        if (obs1.init_csv == False):
            trace_print(4, "Append processing start")
            foreach_obs(weather_obs_app_append, obs1_list)
    if (obs1.collect_data == True):
        run_minutes = 0
        t_begin = ObsDate.now()
        trace_print(4, "starting time: ",
                    t_begin.strftime("%A, %d. %B %Y %I:%M%p"))
        if (obs1.append_data_specified == True):
            foreach_obs(weather_obs_app_start, obs1_list)
        delay_t = 60 - t_begin.minute
        trace_print(4, "minutes till the next hour: ", str(delay_t))
        while True:
            main_obs_loop(obs1_list)


def obs_init_logger():
    # logging.basicConfig(handlers=[logging.NullHandler()],
    logging.basicConfig(stream=sys.stdout,
                        level=logging.DEBUG,
                        format='weather_obs - %(message)s')
    # print(logging.root.handlers)
    # zero out the handlers or install NullHandler above.
    # this inhibts logger from putting out messages to stderr
    # which appears to be double logging when run from cli or foreground.
    logging.root.handlers = []
    # global logger
    global schedule_logger
    logger = logging.getLogger('weather_obs_f')
    ch = logging.StreamHandler(stream=sys.stdout)
    ch_format = logging.Formatter('weather_obs - %(message)s')
    ch.setFormatter(ch_format)
    ch.setLevel(logging.INFO)
    fhandler = TimedRotatingFileHandler('weather_obs.log',
                                        when="d",
                                        interval=1,
                                        backupCount=5)
    formatter_f = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fhandler.setFormatter(formatter_f)
    fhandler.setLevel(logging.DEBUG)
    logger.addHandler(fhandler)
    logger.addHandler(ch)
    logger.addHandler(logging.NullHandler())
    # also put the schedule logger to file
    schedule_logger = logging.getLogger('schedule')
    schedule_logger.setLevel(level=logging.DEBUG)
    schedule_logger.addHandler(fhandler)

def disable_logging_file():
    """
    Disables logging 
    still leave weather_obs.log with zero bytes.
    https://stackoverflow.com/questions/27647077/fully-disable-python-logging
    """
    print(logging.getLogger('weather_obs_f').handlers)
    logging.getLogger('weather_obs_f').setLevel(logging.CRITICAL + 1)
    return



if __name__ == "__main__":
    global obs_time_debug, obs_debut_t_delta, trace
    obs_time_debug = False
    obs_debug_t_delta = 9
    # default global vars.
    # iteration is for duration/repetitive hourly collection - so you know what index you are at.
    trace = True

    obs_init_logger()
#
    weather_obs_app()
