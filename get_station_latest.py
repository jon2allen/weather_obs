#!/usr/bin/env python3
#########################################################################
# get_lastest_obs
#
# pass a station prefix and a extension
# returns the most current by file name format
#
#
#
# output - last file by current day.
#
#########################################################################
import os
import sys
import argparse
from datetime import datetime, timedelta
# from weather_obs import *
import obs_utils


class obsGetStationApp:
    def __init__(self):
        self.start_time = datetime.now()
        self.working_dir = os.getcwd()
        self.parser = argparse.ArgumentParser(
            description='get_station_lastest')

    def set_arg_parser(self):
        self.parser.add_argument(
            "--station_prefix", help='4 character prefix \(ex. KDCA\)')
        self.parser.add_argument(
            "--extension", help='extention type')
        self.parser.add_argument(
            "--outdir", help='directory to search')

    def set_app_arguments(self):
        args = self.parser.parse_args()
        if args.station_prefix == None:
            print("error: no station")
            sys.exit(8)
        if args.station_prefix:
            self.station_id = args.station_prefix
            if len(args.station_prefix) < 1:
                print("No station input")
                sys.exit(8)
        if args.extension:
            self.ext = args.extension
        else:
            self.ext = "csv"
        if args.outdir:
            self.outdir = args.outdir
        else:
            args.outdir = "."

    def run(self):
        self.set_arg_parser()
        self.set_app_arguments()
        file1 = self.get_target_name(self.station_id, self.ext)
        print(file1)

    def get_target_name(self, station_id, ext):
        now = datetime.now()
        obs_cut_time = now
        # set time before - data at :52 and retrieved at 20 after hour
        g1 = obs_utils.create_station_glob_filter(
            station_id, ext, obs_cut_time)
        # print(f"G1: {g1}" )
        target = obs_utils.hunt_for_noaa_files3('.', g1, 'csv')
        # print(f"target: { target }")
        return target


if __name__ == "__main__":
    myapp = obsGetStationApp()
    myapp.run()
