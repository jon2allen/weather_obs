#!/usr/bin/env python



from weather_obs import create_station_file_name
import requests
import pandas as pd
import numpy as pd
from bs4 import BeautifulSoup
import re
from weather_obs import *

tidal_pt = 'https://www.ndbc.noaa.gov/data/Forecasts/FZUS51.KLWX.html'



#leantext = BeautifulSoup(my_f[x:y], "html.parser").text

#print(leantext)

class obs_collector:
     def __init__( self, url, id):
          self.station_url = url
          self.station_id = id
     def show_collector(self):
          return  str(self.station_id + "@" + self.station_url)
     def get_url_data(self):
         self.url_data = requests.get(self.station_url) 
     def show_url_data(self):
          return self.url_data.text
     def set_station_file_name(self):
          self.obs_filename = create_station_file_name(self.station_id, "txt")
     def write_station_data(self):
          obs_file = open(self.obs_filename,"w")
          obs_file.write(self.last_forcast)
          obs_file.close()
     def obs_collection_sequence(self):
         self.get_url_data()
         self.find_station_data()               
         self.set_station_file_name()
         self.write_station_data()
     def find_station_data(self):
          f = BeautifulSoup(self.url_data.text,'lxml')
          my_content = f.find(id="contentarea")
          # print(my_content)
          t = re.finditer(r'\w\w\w\d\d\d\W\d\d\d\d\d\d',  str(my_content) )
          station_list = []
          text_obs = str(my_content)
          #leantext = BeautifulSoup(text_obs,'lxml').text
          #text_obs = leantext
          for i in t:
               index = int(i.start())
               # print(str(index))
               # print(text_obs[index:(index+6)])
               station_name = text_obs[index:(index+6)]   
               station_list.append((index, station_name))
          tail = re.finditer(r'Start Tail', str(my_content))
          for s in tail:
               # print("Start Tail: ", str(s.start()))
               station_list.append((int(s.start() - 5), 'end'))
          print(station_list)
          print(len(station_list))
          station_list_len = len(station_list)
          #print("test_obs:")
          #print(text_obs)
         
          for i in range(station_list_len):
               station_data ="<not found>"
               if i == (station_list_len - 1):
                    break
               #print("i: ", str(i) )
               station_data = text_obs[station_list[i][0]:station_list[i+1][0]]
               #print("station_data:", station_data)
               #print(" self.station:", self.station_id)
               if self.station_id in station_data:
                    my_station = station_data
                    break
          r_station_data  =  BeautifulSoup(station_data,'lxml').text
          self.last_forcast = r_station_data
          return r_station_data
                    
               # print(text_obs[station_list[i][0]:station_list[i+1][0]])
  
          

obs_x = obs_collector( url = 'https://www.ndbc.noaa.gov/data/Forecasts/FZUS51.KLWX.html', id = "ANZ530")



print( obs_x.show_collector())

obs_x.obs_collection_sequence()

#obs_x.get_url_data()

# print(obs_x.show_url_data())

#print(obs_x.station_id)

#obs_x.set_station_file_name()

print(obs_x.obs_filename)

#print("Station data: \n", obs_x.find_station_data())

#obs_x.write_station_data();



exit();    
 

