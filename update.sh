#!/bin/bash

cd /home/njb/skramdykk
mkdir -p log
python3 fetchdata/fetchdata.py >> log/fetch.log
python3 interpolatedives/interpolatedives.py >> log/process.log
python3 timeseries/divetimeseries.py >> log/timeseries.log


