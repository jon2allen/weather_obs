from datetime import datetime
import pytz
from weather_obs import *
import logging
global trace
trace = False

"""
Test harness for station file name 

create_station_file_name ->  create from curent time
create_station_file_name2 -> create from last obervation from feed
"""

global logger
logger = logging.getLogger('weather_obs_f')
#note made changes to weather_obs.py for logging and csv_headers to make work, not sure if perm
#
test_station = "https://w1.weather.gov/xml/current_obs/KDCA.xml"
test_station_hawaii = "https://w1.weather.gov/xml/current_obs/PHOG.xml"

def app_station(i_station):
    w_xml = get_weather_from_NOAA(i_station)
    headers, row = get_data_from_NOAA_xml(w_xml)
    print(str(w_xml))
    #item 9 is the observation_time 
    print(row[9])
    t_str = row[9]
    print(t_str[:-3])
    #actual timezone is not important for obs file output.
    my_date = datetime.datetime.strptime( t_str[:-3], "%B %d %Y, %I:%M %p ")
    print(my_date)
    
app_station(test_station)
app_station(test_station_hawaii)

print( create_station_file_name2( test_station) )
print( create_station_file_name2(test_station_hawaii))
print( create_station_file_name())
