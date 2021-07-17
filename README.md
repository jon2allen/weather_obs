# weather_obs
Python command line script to read XML weather observations and write to csv file

Current version will append data each hour.  Usefule for graphing with excel or python pandas

To Run:

1)  python weather_obs.py  --station https://w1.weather.gov/xml/current_obs/KDCA.xml
    - runs to default date time file.

--init <file>  -- create custom name csv

--append <file> -- append a row of xml data to cusotmer csv

--collect -- continue and append observations in background until stopped
   ( Ctrl-C in foreground, kill in background )


--resume - start appending at current day file.  if no curent day exists. start new one

Run in background on Linux:  python weather_obs.py --init --station https://w1.weather.gov/xml/current_obs/KDCA.xml &
    
    nohup example:   nohup /usr/bin/python3 ./weather_obs.py --file my_obs.txt --cut --collect  > /dev/null  2>&1 < /dev/null &


Where to find stations:

https://w1.weather.gov/xml/current_obs/


CSV default file:  TIST_Y2021 _M02_D09_H19.csv  ( stations + date )


resume.sh - suggested startup scrpt for bootup. change to suit your install.

Examples:
weather_obs.py  --station https://w1.weather.gov/xml/current_obs/KDCA.xml --cut --collect --resume  
** resume cut and collect after reboot. 

Utilites:
  * specific utilites - need some customization for other regions/waters                                                                                                                           
-noaa_tidal_potomac.py -  program to pull from webscrape the marine forecase to text files
-noaa_duplicates.py - using md5 to detect duplicates                                                                                                                       
-daily_weather_chart_obs.py - program to create png chart of wind and table of data in html format      
                                                                                                                            
