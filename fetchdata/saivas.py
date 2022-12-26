
#
""" Saivas Module to load data from a saivas server, decode it and store it to mongodb or some cloud service

(C) 2016 Nils Jacob Berland

"""

from ftplib import FTP
import sys
import os
import os.path
import pymongo
import arrow
from decode import decoder

import logging

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-2s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class SaivasServer(object):
    """ A Saivas Server object manage communication with a FTP server from Saivas
    ...
    """

    def __init__(self, ftpserver, username, password, serverdir, storedir, connstr, database, collection):
        self.ftpserver = ftpserver
        self.username = username
        self.password = password
        self.serverdir = serverdir
        self.storedir = storedir
        self.ftpconn = None
        self.mongodb = pymongo.MongoClient(connstr, uuidRepresentation="standard")[database]
        self.mongocollection = self.mongodb[collection]
        return

    def make_connection(self):
        self.ftpconn = FTP(self.ftpserver)  # connect to host, default port
        return self.ftpconn.login(self.username, self.password)

    def fetchdata(self):

        # open the server and change to correct directory
        # list all files
        logger.debug("Connecting to FTP server")

        if self.ftpconn == None:
            make_connection()

        self.ftpconn.cwd(self.serverdir)
        allfiles = self.ftpconn.nlst()

        # iterate over all files
        for entry in allfiles:
            if entry.find(".txt") > 0:
                # get the file and store it locally if it does not exist locally
                full_path = os.path.join(self.storedir, entry)
                # print(full_path)
                if os.path.isfile(full_path):
                    #  File exists - no need to retrieve
                    pass
                else:
                    try:
                        with open(full_path, "wb") as file_handle:
                            self.ftpconn.retrbinary('RETR ' + entry, file_handle.write)
                            logger.debug("Downloaded %s", full_path)
                    except:
                        logger.debug('Error saving file %s', full_path)
                        os.unlink(full_path)

    def filename2date(self, filename):
        yyyy = '20' + filename[:2]
        mm = filename[2:4]
        dd = filename[4:6]
        return arrow.get(yyyy + '-' + mm + '-' + dd, 'YYYY-MM-DD')

    def storedentries(self):
        return (self.mongocollection.count_documents({}))

    def decodeall(self):
        """
        Attempt to decode all documents and store them to mongo
        """
        for entry in os.listdir(self.storedir):
            full_path = os.path.join(self.storedir, entry)
            if os.path.isfile(full_path) and entry[0] != '.':
                try:
                    mydive = decoder(self.storedir, entry)
                    if mydive.verifydata():
                        mydive.decode()
                        # print(mydive.datadict)
                        if "profilenumber" in mydive.datadict:
                            # hits = self.mongocollection.find({"profilenumber": mydive.datadict["profilenumber"]})
                            # print("Count at database ", hits.count_documents())
                            if self.mongocollection.count_documents({"profilenumber": mydive.datadict["profilenumber"]})==0:
                                self.mongocollection.insert_one(mydive.datadict)
                                logger.debug('Saved %s to mongodb', mydive.datadict["profilenumber"])
                except Exception as e:
                    logger.debug('Error decoding %s (%s)', entry, str(e))

if __name__ == "__main__":
    FTPSERVER = "station.saivas.net"
    USERNAME_Gabriel = "14000000"
    PASSWORD_Gabriel = "apb5"
    SERVERDIR = "14000000/ctd"
    LOCALDIR = "/Users/njberland/PycharmProjects/amalieskram/textfiles/"

    gabrielserver = SaivasServer(FTPSERVER, USERNAME_Gabriel, PASSWORD_Gabriel, SERVERDIR, LOCALDIR)
    # gabrielserver.make_connection()
    # gabrielserver.fetchdata()
    gabrielserver.decodeall()
