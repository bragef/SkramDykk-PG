#!/bin/bash

# cd /local/webdata/gabriel/skramdykk || exit
cd /home/app/skramdykk||exit

mkdir -p log 

## With  --max-age 7 only files with age less than 7 days will be processed.
## Run fetchdata without max-age for initial import
python3 fetchdata/fetchdata.py --max-age 7 &>> log/fetch.log
python3 interpolatedives/interpolatedives.py &>> log/process.log


