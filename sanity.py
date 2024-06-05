"""
  Test routine for the sanity check
  Using an exaple from June 4th, 2021
"""

import datetime
from weather_obs import *

global logger
logger = logging.getLogger('weather_obs_f')

test_data_row = "NOAA's National Weather Service","http://weather.gov/","","15 minutes after the hour","60","Washington/Reagan National Airport, DC, VA","KDCA","38.84833","-77.03417","Jun 4 2021, 12:52 pm EDT","Fri, 04 Jun 2021 12:52:00 -0400","Thunderstorm Light Rain","77.0 F (25.0 C)","77.0","25.0","62","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","1014.0 mb","1014.0","29.94","63.0 F (17.2 C)","63.0","17.2","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","<no_value_provided>","9.00","http://forecast.weather.gov/images/wtf/small/","http://www.weather.gov/data/obhistory/KDCA.html","tsra.png","http://www.weather.gov/data/METAR/KDCA.1.txt","http://weather.gov/disclaimer.html","http://weather.gov/disclaimer.html","http://weather.gov/notice.html"

test_xml = get_weather_from_NOAA("https://forecast.weather.gov/xml/current_obs/KDCA.xml`")

obs1 = ObsSetting("https://forecast.weather.gov/xml/current_obs/KDCA.xml")`

if (obs_sanity_check(obs1, test_xml, test_data_row)):
   print("check failed")
else:
   print("check passed - check for xml file in current running dir")
