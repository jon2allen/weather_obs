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
           cycle   -  "weekly, daily, monthly"
                      Data per CSV file
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
import schedule

def weather_obs_init():
    parser = argparse.ArgumentParser(description='NOAA weather obsevation')
    parser.add_argument('--init', help='Initialize CSV' )
    parser.add_argument('--station', help='URL of station' )
    parser.add_argument('--collect', help='Run collectiion in background - Y/N', action="store_true")
    parser.add_argument('--append', help='Append data to CSV file - specifed' )
    parser.add_argument('-d', '--Duration', help='Always, 1Day, 1week, 1Month' )
    args = parser.parse_args()
    # cannocial header
    # can't depend on xml feed to complete every value
    global csv_headers
    csv_headers = ['credit','credit_URL','image','suggested_pickup','suggested_pickup_period','location','station_id','latitude','longitude','observation_time','observation_time_rfc822','weather','temperature_string','temp_f','temp_c','relative_humidity','wind_string','wind_dir','wind_degrees','wind_mph','wind_kt','wind_gust_mph','wind_gust_kt','pressure_string','pressure_mb','pressure_in','dewpoint_string','dewpoint_f','dewpoint_c','heat_index_string','heat_index_f','heat_index_c','windchill_string','windchill_f','windchill_c','visibility_mi','icon_url_base','two_day_history_url','icon_url_name','ob_url','disclaimer_url','copyright_url','privacy_policy_url']

    global append_data
    append_data = False
    if (args.append):
        append_data = True
    global collect_data
    collect_data = False
    if (args.collect):
      collect_data = True
    # check station and fill out appropriate values
    if (args.station):
      global primary_station
      global station_file
      global station_id
      primary_station = args.station
      station_file = ""
      station = args.station[-8:]
      print("Station URL of XML",args.station)
      print(station[:4])
      station_id = station[:4]
      print("Station id:  ", station_id)
      year, month, day, hour, min = map(str, time.strftime("%Y %m %d %H %M").split())
      station_file = station_id + '_Y' + year + '_M' + month + '_D' + day + '_H' + hour + ".csv"
      print("Satation filename: ", station_file)
      global init_csv
      init_csv = True
      # initialize a CSV until we prove we are appending.
      if (args.init):
          init_csv = True
          station_file = args.init
          print("init_csv", station_file )
      if (append_data == True):
          station_file = args.append
          init_csv = False
          print( "Station id ( append ): ", station_file )
    else:
      print("Error: No station given - please use --station")
      print(" see readme")
      exit(4)		 
    return True
# default global vars.
# iteration is for duration/repetitive hourly collection - so you know what index you are at. 
obs_iteration = 0 
dump_xml_flag = False
trace = True
primary_station = ""
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
      trace string
 outputs:
      function trace string.
"""
def trace_print( s ):
    global trace
    if ( trace == True):
        print("function trace: ", s)
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
   trace_print('csv_driver')
   if ( mode != 'w' ) and  ( mode != 'a' ):
     print(" mode is invalid")
     return False
   if (len(csv_file) < 4 ):
     print("CSV file must contain station name")
     return False
   ## test for xmldata... not sure yet
   ## newline parm so that excel in windows doesn't have blank line in csv
   ## https://stackoverflow.com/questions/3348460/csv-file-written-with-python-has-blank-lines-between-each-row
   with open(csv_file, mode, newline='') as weather_file:
        weather_writer = csv.writer(weather_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        if ( mode == 'w' ):
            trace_print("csv_driver: header+row")
            weather_writer.writerow( w_header )
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
  if ( append_data != True):
      content = get_weather_from_NOAA(primary_station)
      #  print(content)
      xmld1 = get_data_from_NOAA_xml( content)
      weather_csv_driver('w', station_file, xmld1[0], xmld1[1])
      trace_print("Initializing new file: ")
      trace_print(station_file)
  if ( collect_data == True):
      schedule.every().hour.do( weather_collect_driver, primary_station, station_file)
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
if __name__ == "__main__":
  weather_obs_init()
  if (init_csv == True):
      print("Init... ")
      weather_obs_app_start()
  if (append_data == True):
      print("Appending data")
      weather_obs_app_append()
  if (collect_data == True ):
     global run_minutes
     run_minutes = 0
     weather_obs_app_start()
     while True:
        schedule.run_pending()
        run_minutes += 1
        if ((run_minutes % 60) == 0):
            print("Num minutes running: ", run_minutes )
        if ( run_minutes == 1440 ):
            print("Running 1 day")
        time.sleep(60)
        
      
