# weather_obs
Python command line script to read XML weather observations and write to csv file

Current version will append data each hour.  Usefule for graphing with excel or python pandas

To Run:

python weather_obs.py --init --station https://w1.weather.gov/xml/current_obs/KDCA.xml

Run in background on Linux:  python weather_obs.py --init --station https://w1.weather.gov/xml/current_obs/KDCA.xml &

Where to find stations:

https://w1.weather.gov/xml/current_obs/


CVS file:  TIST_Y2021 _M02_D09_H19.csv  ( stations + date )
