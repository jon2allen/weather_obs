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

--duration ( not implemented yet )   


Run in background on Linux:  python weather_obs.py --init --station https://w1.weather.gov/xml/current_obs/KDCA.xml &

Where to find stations:

https://w1.weather.gov/xml/current_obs/


CSV default file:  TIST_Y2021 _M02_D09_H19.csv  ( stations + date )
