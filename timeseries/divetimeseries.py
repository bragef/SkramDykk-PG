"""
Code to resample dives in time. The code can make

(c) NJB 2016

"""
import pymongo
import pandas as pd
import datetime
from numpy import nan
import logging
import arrow
import json
from bson import json_util

depth_set = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5,
             19.5]

# get the logging OK
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-2s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

with open("config.json","r") as f:
    configdata = json.loads(f.read())

client = pymongo.MongoClient()
mongodb = client[configdata["mongodbname"]]
fromcoll = mongodb[configdata["interpolatatedcollection"]]
tocoll = mongodb[configdata["resampledcollection"]]

#fromcoll = pymongo.MongoClient().saivasdata.diveinterpolated
#tocoll = pymongo.MongoClient().saivasdata.resampled

def updatetimeseries(force=False, timeframe='D', datatype='temp'):
    """ code to resample ALL dives by time
    """

    templist = []

    try:
        # get all interpolated dives from Mongo and load them in a python list
        for cursor in fromcoll.find().sort('startdatetime', pymongo.ASCENDING):  # .limit(100):

            interpolated = cursor["timeseries"]
            # get the time and temp values for each depth
            item = {"ts": cursor['startdatetime']}
            try:
                for depth in interpolated:
                    item[str(depth['pressure(dBAR)'])] = depth[datatype]
                templist.append(item)
            except:
                print("datatype {} missing at time {}".format(datatype, cursor['startdatetime']))
        # load the list into a dataframe and resample
        df = pd.DataFrame(list(templist))
        df.index = df['ts']
        df = df.sort_index()
        df = df.resample(timeframe).mean()
        # the the time back as an column
        df['ts'] = df.index

        # the following code is an HACK and should be rewritten
        # convert the dataframe to a LIST with dictionaries
        d = df.to_dict(orient='record')
        logger.debug("Resampled %s %s", timeframe, datatype)
        # convert the list into a document that can be stored and store it to Mongo
        for item in d:
            divedata=[]
            for key, value in item.items():
                if key != 'ts':
                    divedata.append({"pressure(dBAR)":float(key),datatype:value})

            new = {'ts': pd.to_datetime(item['ts']), 'timeframe': timeframe, "datatype": datatype, 'divedata':divedata}
            tocoll.update({"timeframe": timeframe, "datatype": datatype, 'ts': pd.to_datetime(item['ts'])}, new, upsert=True)
    except:
        logger.debug("Error creating timeseries %s",datatype)
    return

if __name__ == "__main__":
    for datatype in ["fluorescens", "temp", "salt", "oxygene",  "turbidity"]:
        print("Timeseries - Datatype ", datatype)
#        updatetimeseries(force=False, timeframe='MS', datatype=datatype)
#        updatetimeseries(force=False, timeframe='D',  datatype=datatype)
#        updatetimeseries(force=False, timeframe='W',  datatype=datatype)
        updatetimeseries(force=False, timeframe='3H', datatype=datatype)