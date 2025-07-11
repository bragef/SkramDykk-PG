"""

Code to process raw dive data from mongodb
into interpolated dive data

Raw data are in the gabriel collection
Interpolated data are in the dives collection

The dives collection will have data organised as follows:
 * type - oxygen, temp, ...
 * startdatetime
 * airtemp
 * devicename
 * filename
 * location
 * profilenumber

(c) NJB 2016

"""

import pandas as pd
from numpy import nan
import numpy as np
import logging
import psycopg2
import json

# no errorhandling 
with open("config.json","r") as f:
    configdata = json.loads(f.read())
PG_CONNSTR = configdata["pg_conn"]

depth_set = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5,
             19.5]

# get the logging OK
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-2s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def processraw(conn, depth_set, force=False):
    count = 0

    with conn.cursor() as cursor:
        # Fetch all sessions
        cursor.execute("""
            SELECT sessionid, COUNT(interpolated_timeseries.sessionid) 
            FROM session_data LEFT JOIN interpolated_timeseries USING(sessionid) 
            GROUP BY sessionid  
            ORDER BY startdatetime DESC;
        """)
        rows_sessions = cursor.fetchall()

        for row in rows_sessions:
            sessionid = row[0]  
            exists = row[1] > 0
            if exists == 0 or force:
                try:
                # Fetch raw data for the session
                    cursor.execute("""
                        SELECT seq, salinity, temperature, pressure_dbar, oxygen, fluorescence, turbidity 
                        FROM raw_timeseries 
                        WHERE sessionid = %s ORDER BY seq;
                    """, (sessionid,))
                    rows_all  = cursor.fetchall()
                    df = pd.DataFrame(rows_all, columns=[desc.name for desc in cursor.description])
                    # make the pressure the index of the dataframe
                    df.set_index('pressure_dbar', inplace=True)
                    # and get rid of all readings after we have been to the bottom
                    df = df.iloc[:df.index.argmax() + 1]
                    # Interpolere hver observasjon
                    for x in depth_set:
                        if x not in df.index and x < df.index.max():
                            df.loc[x] = np.nan

                    df.sort_index(inplace=True)
                    df = df.interpolate(method='index', axis=0).ffill(axis=0).bfill(axis=0)

                    # Remove rows not in depth_set
                    df = df[df.index.isin(depth_set)]

                    # Save iterated values to the database
                    for index, interpolated_row in df.iterrows():
                        cursor.execute("""
                            INSERT INTO interpolated_timeseries (sessionid, seq, salinity, temperature, pressure_dbar, oxygen, fluorescence, turbidity)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                        """, (
                            sessionid,
                            interpolated_row.get('seq'),  
                            interpolated_row.get('salinity'),
                            interpolated_row.get('temperature'),
                            index, 
                            interpolated_row.get('oxygen'),
                            interpolated_row.get('fluorescence'),
                            interpolated_row.get('turbidity')
                        ))
                    
                    count += 1
                    # logger.debug("Processed session %s with %d interpolated readings", sessionid, len(df))
                except Exception as e:
                    logger.error("Error processing session %s: %s", sessionid, e)
                    continue            
    conn.commit() 
    return count


if __name__ == "__main__":
    conn = psycopg2.connect(PG_CONNSTR)
    count = processraw(conn, depth_set, force=False)
    print("Processed {} documents".format(count))
