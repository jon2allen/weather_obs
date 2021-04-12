#!/bin/bash

_now=$(date "+%Y_%m_%d-%H.%M.%S")

echo "Resume now at $_now..."

_file="weather_obs_log.$_now"

echo "file:  $_file"

_working_dir="/var/www/html/weather_obs"

cd $_working_dir

echo "Present working dir: `pwd`"

./weather_obs.py --station https://w1.weather.gov/xml/current_obs/KDCA.xml  --cut --collect --resume > $_file &

