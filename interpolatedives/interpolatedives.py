"""

Code to process raw dive data from mongodb
into interpolated dive data

Raw data are in the gabriel collection
Interpolated data are in the dives collection

The dives collection will have data organised as follows:
 * type - oxygene, temp, ...
 * startdatetime
 * airtemp
 * devicename
 * filename
 * location
 * profilenumber

(c) NJB 2016

"""

import pymongo
import pandas as pd
from numpy import nan
import logging
import json

# no errorhandling 
with open("config.json","r") as f:
    configdata = json.loads(f.read())

client = pymongo.MongoClient(configdata["mongoconn"], uuidRepresentation="standard")
mongodb = client[configdata["mongodbname"]]
fromcoll = mongodb[configdata["rawcollection"]]
tocoll = mongodb[configdata["interpolatatedcollection"]]

depth_set = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5,
             19.5]



# get the logging OK
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-2s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)



# function to process raw dive data to interpolated data
# force implies to not overwrite a processed entry if it already exists
def processraw(force=False):

    count = 0

    # get all raw dive data
    for cursor in fromcoll.find().sort('startdatetime',pymongo.DESCENDING):  # .limit(100):

        rawdata = cursor["rawtimeseries"]
        sessionid = cursor['sessionid']
        if tocoll.count_documents({'sessionid':sessionid}) == 0 or force:
            # create a Pandas Dataframe
            df = pd.DataFrame(list(rawdata))

            # make the pressure the index of the dataframe
            df.index = df['pressure(dBAR)']

            # and get rid of all readings after we have been to the bottom
            df = df.iloc[:df.index.argmax()]

            # and now we should interpolate each observation
            for x in depth_set:
                if x not in df.index and x < df.index.max():
                    df.loc[x] = nan
            df = df.sort_index()
            df = df.interpolate(method='index', axis=0).ffill(axis=0).bfill(axis=0)

            # now get rid of all rows that is not in the depth_set
            query = 'index in ' + json.dumps(depth_set)
            df = df.query(query)

            json_data = df.to_json(orient='records')
            newdata = cursor
            newdata.pop("rawtimeseries", None)
            newdata.pop("_id", None)
            newdata['timeseries'] = json.loads(json_data)

            # print(newdata)
            tocoll.update_many({'sessionid': newdata['sessionid']}, {"$set":newdata}, upsert=True)
            count += 1
    return count

if __name__ == "__main__":

    count = processraw(force=False)
    print("Processed {} documents".format(count))
