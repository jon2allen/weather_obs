#############################################################################################
#
# name:  xml_push.py
# author:  Jon Allen
#
# Purpose:  to place hourly xml data created from obs_csv_to_xml.py for testing
#
#
#  This is meant to be tailored for testing.
#
#  3/13/2023 - used to test DST issue and changes made for 
#              for problems found on 3/12 - tzlocal()
#
#  testomg back in time - use virtual machine and set clock
#  freezgun - seems to have some issues in sub modules that haven't been worked out.
#
#  copy seed xml file to target for weather_obs.py - might be a future fix in this.
#
###########################################################################################

import os
import shutil
from obs_utils import get_noaa_any_files
import schedule
import time

####
#  source_dir is where the xml files located
#  target dir is apache directory where curl or weather_obs.py can pickup
#
#  ./weather_obs.py --station http://localhost/KDCA2.xml
#
#   ( station will be interpreted as DCA2 - but ok for some tests
#
###
source_dir = "/home/jon2allen/testdst/weather_obs/march/"
target_dir = "/usr/local/www/apache24/data/"
xml_list = get_noaa_any_files("/home/jon2allen/testdst/weather_obs/march", "KDCA", "xml" )

print( xml_list )

def copy_func( xml_list): 
   poper = xml_list.pop(0)
   shutil.copy(source_dir + poper, target_dir + "KDCA2.xml" )

   print( "copied: ", poper ) 


schedule.every(1).minutes.do(copy_func,xml_list)

while True:
   schedule.run_pending()
   time.sleep(5)
   if len(xml_list) == 0:
     print("finished")
     quit()

