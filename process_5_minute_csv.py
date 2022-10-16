#!/usr/bin/env python3

"""
Program:  process_5_minute_csv.py

Purpose:  to analyze observations and generate a file that only shows collection times that have
          changed
          
Process file created by following command running via cron


[jon2allen@freebsd12_3 ~]$ crontab -l
#simple unix/linux crontab file format:
#--------------------------------------------------
# min,hour,dayOfMonth,month,dayOfWeek command
#
# field          allowed values
# -----          --------------
# minute         0-59
# hour           0-23
# day of month   1-31
# month          1-12 (or names, see below)
# day of week    0-7 (0 or 7 is Sun, or use names)
#
#--------------------------------------------------
0,5,10,15,20,25,30,35,40,45,50,55  * * * * /home/jon2allen/weather_tracker.sh
[jon2allen@freebsd12_3 ~]$ cat weather_tracker.sh
#!/usr/local/bin/bash
/home/jon2allen/github/weather_obs/weather_obs.py --append /home/jon2allen/jontest.csv --station https://w1.weather.gov/xml/current_obs/KDCA.xml --collectdate --ignoredup > /dev/null


Goal:  find the minutes after tha hour when the observation gets posted.  Run metrics.




"""

from collections import namedtuple
import csv
import obs_time

def read_obs_csv(file_name):
    with open(file_name, 'rU') as data:
        h1 = data.readline().split(",")
        print( h1 )
       # obs_header = namedtuple('obs_heder', data.readline().strip('\" \''))
        new_head = []
        for i in h1:
            i = i.strip()
            i = i.strip('\"')
            i = i.replace(" ", "_")
            new_head.append(i)
        print(new_head)
        
        obs_header = namedtuple('obs_header', new_head)
        reader = csv.reader(data)  # Create a regular tuple reader
        for row in map(obs_header._make, reader):
            yield row

if __name__ == "__main__":
    OBS_INPUT = "jontest.csv"
    OBS_OUTPUT = "jontest_out.csv"
    with open(OBS_OUTPUT, "w", newline='') as d1:
        row = next(read_obs_csv(OBS_INPUT))
        writer = csv.writer(d1)
        writer.writerow((list(row._fields)))
        i = 0
        prior = ""
        for row in read_obs_csv(OBS_INPUT):
            if prior != str(row.observation_time):
                print("prior: ", prior)
                print(row.observation_time)
                if prior != "":
                    obs1 = obs_time.ObsDate(row.collection_time)
                    obs1.emit_type('excel')
                    row = row._replace(collection_time = str(obs1))
                    writer.writerow(list(row))
                prior = row.observation_time
            i = i+1
            if i > 10000:
                break
      

