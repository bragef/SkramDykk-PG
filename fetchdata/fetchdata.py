#
"""
(c) Nils Jacob Berland 2016 - www.sensar.io

"""
import os
from saivas import SaivasServer
import json

# no errorhandling 
with open("config.json","r") as f:
    configdata = json.loads(f.read())

LOCALDIR = configdata["LOCALDIR"]
FTP_SERVER = configdata["FTP_SERVER"]
FTP_USERNAME = configdata["FTP_USERNAME"]
FTP_PASSWORD = configdata["FTP_PASSWORD"]
FTP_SERVERDIR = configdata["FTP_SERVERDIR"]
PG_CONN = configdata["pg_conn"]

if __name__ == "__main__":

    # check if the directory exists first and make it if not
    if not os.path.exists(LOCALDIR):
        os.makedirs(LOCALDIR)
    gabrielserver = SaivasServer(FTP_SERVER,FTP_USERNAME, FTP_PASSWORD, FTP_SERVERDIR, LOCALDIR, PG_CONN)
    gabrielserver.make_connection()
    gabrielserver.fetchdata()
    gabrielserver.decodeall()



