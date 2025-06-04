#!/bin/bash

# cd /local/webdata/gabriel/skramdykk || exit
cd /home/app/skramdykk ||exit

mkdir -p log 

python3 fetchdata/fetchdata.py &>> log/fetch.log
python3 interpolatedives/interpolatedives.py &>> log/process.log
# python3 timeseries/divetimeseries.py >> log/timeseries.log


