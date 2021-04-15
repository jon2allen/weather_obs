#!/usr/bin/python3
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


import sys
import argparse
import csv
import urllib.request
import xml.etree.ElementTree as ET
import time
import datetime
import schedule
"""
testing
"""
from freezegun import freeze_time
import unittest

freezer = freeze_time("2021-02-28 23:56:30", tick=True)
freezer.start()
"""
   set the format from current date
   cannoical format for obs
"""
def create_station_file_name():
      t_now = datetime.datetime.now()
      year, month, day, hour, min = map(str, t_now.strftime("%Y %m %d %H %M").split())
      file_n = station_id + '_Y' + year + '_M' + month + '_D' + day + '_H' + hour + ".csv"
      return file_n
"""
   Get arguments
   Establish globals
"""
def weather_obs_init():
    parser = argparse.ArgumentParser(description='NOAA weather obsevation')
    parser.add_argument('--init', help='Initialize CSV' )
    parser.add_argument('--station', help='URL of station' )
    parser.add_argument('--collect', help='Run collectiion in background - Y/N', action="store_true")
    parser.add_argument('--append', help='Append data to CSV file - specifed' )
    parser.add_argument('-d', '--duration', help='Duration cycle - default - 24 hours, 3 hours')
    parser.add_argument('-c', '--cut', action="store_true")
    parser.add_argument('-x', '--xml', action="store_true")
    args = parser.parse_args()
    trace_print("parsing args...")
    # cannocial header
    # can't depend on xml feed to complete every value
    global csv_headers
    csv_headers = ['credit','credit_URL','image','suggested_pickup','suggested_pickup_period','location','station_id','latitude','longitude','observation_time','observation_time_rfc822','weather','temperature_string','temp_f','temp_c','relative_humidity','wind_string','wind_dir','wind_degrees','wind_mph','wind_kt','wind_gust_mph','wind_gust_kt','pressure_string','pressure_mb','pressure_in','dewpoint_string','dewpoint_f','dewpoint_c','heat_index_string','heat_index_f','heat_index_c','windchill_string','windchill_f','windchill_c','visibility_mi','icon_url_base','two_day_history_url','icon_url_name','ob_url','disclaimer_url','copyright_url','privacy_policy_url']
    #  global collect_data
    #  collect_data = False
    #  global job1
    global collect_data
    
    global job1
    global cut_file
    global append_data
    global append_data_specified
    global init_csv
    global duration_interval
    global dump_xml_flag
    dump_xml_flag = False
    init_csv = False
    cut_file = False
    append_data_specified = False
    apppend_data = False
    collect_data = False
    duration_interval = 0
    if (args.duration):
       duration_interval = int(args.duration)
       trace_print("duration interval: ", str(args.duration))
    if (args.xml == True):
        dump_xml_flag = True   
    if (args.cut):
       trace_print("cut specified")
       cut_file = True
       append_data = False
    if (args.append):
        trace_print("appdend specified")
        append_data = True
        append_data_specified = True
       # collect asssumes append
    # check station and fill out appropriate values
    if (args.station):
      global primary_station
      global station_file
      global station_id
      primary_station = args.station
      station_file = ""
      station = args.station[-8:]
      trace_print("Station URL of XML - " + str(args.station))
      # print(station[:4])
      station_id = station[:4]
      trace_print("Station id:  ", station_id)
      station_file = create_station_file_name()
      trace_print("Satation filename: ", station_file)
      init_csv = True
      # initialize a CSV until we prove we are appending.
      if (args.init):
          init_csv = True
          station_file = args.init
          trace_print("init_csv", station_file )
      if (append_data_specified == True):
          station_file = args.append
          init_csv = False
          trace_print( "Station id ( append ): ", station_file )
      if (args.collect):
         trace_print("collect in station setting")
         collect_data = True
         job1 = ""
         if (init_csv == False) and (append_data_specified == False):
            station_file = create_station_file_name()
            trace_print("Station filename (collect): ", station_file)
    else:
      trace_print("Error: No station given - please use --station")
      trace_print(" see readme")
      exit(4)		 
    return True
# default global vars.
# iteration is for duration/repetitive hourly collection - so you know what index you are at. 
obs_iteration = 0 
trace = True
primary_station = ""
job1 = ""
append_data = False
"""
 function: dump_xml
   purpose:  dump raw xml for debugging
    inputs:
      raw xmldata
      iteration of dump
 outputs:
      file:  xml_dump + <iteration> . xml
"""
def dump_xml( xmldata, iteration ):
    trace_print("dumpxml_entry")
    global dump_xml_flag
    if ( dump_xml_flag == True):
        trace_print("dump_xml")
        file = "xml_dump" + str(iteration) + ".xml"
        fh = open(file, 'wb')
        fh.write( xmldata)
        fh.close()      
"""
 function: trace_print
    tracing to stdout with flag
 inputs:
      trace string + list of stuff that can 
      be converted into strings.
 outputs:
      function trace string.
"""
def trace_print( s, *t1):
    global trace
    jstr = ''.join(t1)
    if ( trace == True):
        print("function trace: ", s, jstr, flush=True)
"""
 function: get_weather_from_NOAA(xmldata)
 inputs:
      station - URL of station XML
 outputs:
      tuple ( header, data ) - for updating CSV file
"""

def get_weather_from_NOAA(station):
   trace_print("url request")
   with urllib.request.urlopen(station) as response:
   	xml = response.read()
   return xml
"""
   function:  transforam observation
     - puts data into more usuable format for excel or pandas
   inputs:  xml attribute, data
   outputs:  data for writing out to CSV
"""  
def transform_observation( attribute, data):
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
def get_data_from_NOAA_xml( xmldata ):
  tree = ET.fromstring(xmldata)
  h1 = []
  r1 = []
  r1_final = []
  global csv_headers
  trace_print("parsing NOAA xml")
  for child in tree:
     h1.append(child.tag)
     r1.append(child.text)
  for ch in csv_headers:
    if not r1:
      r1_final.append('')
    elif ( ch in h1 ):
      r1_final.append(transform_observation( ch, r1.pop(0)))
    else:
      r1_final.append('<no_value_provided>')
  h1 = csv_headers
  return h1,r1_final   
"""
 function: weather_csv_driver
 inputs:
      mode = "w" for write or "a" for append
      csv_file = fully formed file name
      w_header, w_row  = weather xml data from NOAA in
                 in binary string - broken into lists
"""
def weather_csv_driver( mode, csv_file, w_header, w_row ):
   cut_mode = False
   trace_print('csv_driver')
#   if ( mode != 'w' ) and  ( mode != 'a' ):
#     trace_print(" mode is invalid")
#     return False
   if (len(csv_file) < 4 ):
     print("CSV file must contain station name")
     return False
   if ( mode == 'c'):
     # cut file request is active
     # denote the special mode and change it to write.
     cut_mode = True
     mode = 'w'
   ## test for xmldata... not sure yet
   ## newline parm so that excel in windows doesn't have blank line in csv
   ## https://stackoverflow.com/questions/3348460/csv-file-written-with-python-has-blank-lines-between-each-row
   with open(csv_file, mode, newline='') as weather_file:
        weather_writer = csv.writer(weather_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        if ( mode == 'w' ):
            trace_print("csv_driver: header+row")
            weather_writer.writerow( w_header )
            if (cut_mode == False):
              trace_print("csv_driver: header_only")
              weather_writer.writerow( w_row )
        elif ( mode == 'a' ):
            trace_print("csv_drver: row_only")
            weather_writer.writerow( w_row )
   weather_file.close()
   return True 
"""
function: weather_collet_driver
 inputs:
      xml_url = URL to station XML
      csv_out = output file
 outputs:  returns TRUE
      Appends ( only ) csv file with data from obs xml
"""
def weather_collect_driver( xml_url, csv_out):
     global obs_iteration
     trace_print("weather_collect_driver")
     xmldata = get_weather_from_NOAA( xml_url )
     outdata = get_data_from_NOAA_xml( xmldata )
     weather_csv_driver('a', csv_out, outdata[0], outdata[1])
     obs_iteration = obs_iteration + 1
     dump_xml(xmldata, obs_iteration )
     return True
# early test hardness code 
# import obstest

# default is to schedule for now
# TODO - need init the file. collect driver needs the file to exits
# primitive_test_loop()
def weather_obs_app_start():
  # if appending and scheduling - skip over to collect
  global append_data
  if ( append_data != True):
      content = get_weather_from_NOAA(primary_station)
      #  print(content)
      xmld1 = get_data_from_NOAA_xml( content)
      weather_csv_driver('w', station_file, xmld1[0], xmld1[1])
      trace_print("Initializing new file: ")
      trace_print(station_file)
  if ( collect_data == True):
      global job1
      trace_print("schedule")
      append_data = True
      job1 = schedule.every().hour.do( weather_collect_driver, primary_station, station_file)
  return
#
#
# need to specify file.
def weather_obs_app_append():
  content = get_weather_from_NOAA(primary_station)
  #  print(content)
  xmld1 = get_data_from_NOAA_xml( content)
  weather_csv_driver('a', station_file, xmld1[0], xmld1[1])
  return
#
"""
  Pass last cut time and check against now
  check at each day, month, and year.
  hour cycle is every so many hours
  No need to support less - observations are hourly
  """
def duration_cut_check( t_last, hour_cycle  ):
  trace_print("Duration check")
  t_now = datetime.datetime.now() 
  if t_now.year > t_last.year:
      trace_print( "Duration year check")
      return True
  if t_now.month > t_last.month:
      trace_print( "Duration month check")
      return True
  if t_now.day > t_last.day:
      trace_print( "Duration day check")
      return True
  if (t_now.hour - t_last.hour == 0 ):
      return False  
  if (hour_cycle > 0):
     if ((t_now.hour - t_last.hour) % hour_cycle == 0 ):
       trace_print( "Duration cycle check at ", str(hour_cycle))
       return True
  return False     

if __name__ == "__main__":
  #freezer = freeze_time("2021-02-28 23:15:30", tick=True)
  # freezer.start()
  weather_obs_init()
  if (init_csv == True):
      trace_print("Init... ")
      weather_obs_app_start()
  if (append_data_specified == True):
      trace_print("Appending data")
      weather_obs_app_append()
  if (collect_data == True ):
     global run_minutes
     run_minutes = 0
     t_begin = datetime.datetime.now()
     trace_print("starting time:",t_begin.strftime("%A, %d. %B %Y %I:%M%p"))
     # for case of collect and append specified
     # TODO - should we force to be 15 minutes afer the hour
     #        NOAA may not update.  Or should that be an option??
     if (append_data_specified == True):
         weather_obs_app_start()
     delay_t = 60 - t_begin.minute
     trace_print("minutes till the next hour: ", str(delay_t))
     while True:
        run_minutes =  datetime.datetime.now().minute
        trace_print("run_minutes(tst): ", str(datetime.datetime.now().minute))
        if ((run_minutes % 60) == 0):
            # every hour check to see if need to cut
            # cut check before schedule.run_pending
            trace_print("Num minutes running: ", str(run_minutes) )
            if ( cut_file == True):
                t_cut_time = datetime.datetime.now()
                if ( duration_cut_check(t_begin, duration_interval)): 
                #    if ((t_begin.minute - t_cut_time.minute) < 5):
                #       trace_print(" cut time less than 1 hour")
                #        if (duration_interval < 2 ):
                #           schedule.cancel_job(job1)
                #        else:
                #           job1.run()
                #           trace_print(" Ran job again to catch up ")
                    trace_print("running cut operation")
                    station_file = create_station_file_name()
                    trace_print("New Station file:", station_file)
                    #create new file with cannocial headers
                    weather_csv_driver('c', station_file, csv_headers, [])
                    schedule.cancel_job(job1)
                    # we rassigned the next station file ( global )
                    # new writes should go there.
                    t_begin = datetime.datetime.now() 
                    trace_print("Time of last cut:",t_begin.strftime("%A, %d. %B %Y %I:%M%p"))
                    # this will reschedule job with new file.
                    weather_obs_app_start()
        else:
             schedule.run_pending()            
        time.sleep(60)