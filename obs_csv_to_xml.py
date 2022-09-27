#!/usr/bin/python3
import os
import argparse
from datetime import date, datetime, timedelta
from dateutil import parser
from dateutil.parser import ParserError
import calendar
from obs_utils import *
from weather_obs import create_station_file_name
import pprint
import xml.etree.ElementTree
import obs_time
from timezonefinder import TimezoneFinder
import pytz
import copy

class ObsCsvToXml:
    def __init__( self, csv_file, xml_path ):
        self.csv_file = csv_file
        self.xml_path = xml_path
        self.df = None
        self.xmlprint = False
        self.xml_template = xml.etree.ElementTree.parse('template.xml')
        self.xml_template_root = self.xml_template.getroot()

    def read_csv( self):
        trace_print(4, "reading: ", self.csv_file)
        self.df = read_weather_obs_csv(self.csv_file)
    
    def set_time_zone( self):
        row = self.df.loc[1].to_list()
        tf = TimezoneFinder()
        # print("row values", row[8], row[7])
        t_tz = tf.timezone_at(lng=float(row[8]), lat=float(row[7]))
        #print("t_tz:" , t_tz)
        self.tztext = t_tz
        my_zone = pytz.timezone(self.tztext)
        #print("zone: " , my_zone.zone)
        obs_tz_time = obs_time.ObsDate(row[9])
        obs_tz_time.emit_type("dt")
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        #print("obs_tz_time:" , obs_tz_time)
        loc_dt = my_zone.localize(obs_tz_time)
        self.timezone = loc_dt.strftime(fmt)[-3:]
        #print(self.timezone)
    
    def create_xml_file( self, station, obs_t):
        obs_t.emit_type("dt")
        return create_station_file_name( station=station, ext='xml', obs_time_stamp = obs_t)
        
        
    def update_observation_time(self, obs_t):
        template_t = "Last Updated on "
        obs_new = template_t + str( obs_t ) 
        obs_new = obs_new + " " + self.timezone
        return obs_new
        
        
    def update_template(self, header, row):
        # csv is out of order from xml.
        # use zip to create dictionary
        h = header.to_list()
        r = row.to_list()
        upt_dict = dict(zip(h,r))
        # make a deep copy since we will be called many time.
        xml_tmp = copy.deepcopy(self.xml_template)
        xml_tmp_root = xml_tmp.getroot()
        remove_lst = []         
        for k in upt_dict.keys():
            if k == "observation_time":
                obs_t = obs_time.ObsDate(upt_dict[k])
                obs_t.emit_type("dt")
                obs_temp = self.update_observation_time(obs_t)
                el = xml_tmp_root.find(k)
                el.text = str(obs_temp)
            else:
                try:
                    el = xml_tmp_root.find(k)
                    el.text = str(upt_dict[k])
                except:
                    trace_print(4, "error: ",  "k: ", k)
                    # most likely - csvfile was edited post creation
                    # columns added. or some other column error.
        for child in xml_tmp_root:
            # defer updates to end. 
            # very bad to update xml data struct while iterating it.
            if (str(upt_dict[child.tag]).strip()) == "nan":
                trace_print(1, "remove queue:", child.tag)
                remove_lst.append(child)
        while remove_lst:
            c_ptr = remove_lst.pop()
            trace_print(1, "removing ", c_ptr.tag)
            xml_tmp_root.remove(c_ptr)
            
        xml_file = self.create_xml_file(upt_dict["station_id"], obs_t)
        trace_print(4, "writing xml: ", xml_file)
        xml_tmp.write(xml_file)
        if self.xmlprint == True:
            self.test_xml( xml_file)
        
    def test_xml(self, file):
        print("testing...", file)
        xml_tmp = xml.etree.ElementTree.parse(file)
        xml_tmp_root = xml_tmp.getroot()
        for child in xml_tmp_root:
            trace_print(1, "tag: ", child.tag)
            trace_print(1, "text: ", child.text)
    
    def run(self):
        self.read_csv()
        self.set_time_zone()
        for i in range(1,self.df.shape[0]):
            self.update_template(self.df.columns, self.df.loc[i])
        
class obsXMLAPP:
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.working_dir = os.getcwd()
        self.parser = argparse.ArgumentParser(description='obs_csv_to_xml_utility')

    def set_arg_parser(self):
        self.parser.add_argument("--csvfile", help='csvfile to covert')
        self.parser.add_argument("--outdir", help='dir to write xml data')
    
    def set_app_arguments(self):
        args = self.parser.parse_args()
        if args.csvfile:
            self.csvfile = args.csvfile
        else:
            trace_print(4, "no csvfile specified")
            sys.exit(8)
        if args.outdir:
            self.outdir = args.outdir
        else:
            trace_print(4, "no outdir specified")
            sys.exit(8)
    
    def run(self):
        self.set_arg_parser()
        self.set_app_arguments()
        my_instance = ObsCsvToXml(self.csvfile, self.outdir)
        my_instance.run()
             
         

            
        
        
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO,
                        format='obs_csv_to_xml.py - %(message)s')
    logger = logging.getLogger('weather_obs_f')

    
    myapp = obsXMLAPP()
    myapp.run()
         
   
    
    