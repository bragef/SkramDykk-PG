#
""" Saivas Module to load data from a saivas server, decode it and store it to mongodb or some cloud service
(C) 2016 Nils Jacob Berland
Updated to use PostgreSQL by Brage Førland 2025
"""

from ftplib import FTP
import sys
import os
import os.path
import psycopg2, psycopg2.extras
import arrow
from decode import Decoder
import logging

psycopg2.extras.register_uuid()

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
    def __init__(self, ftpserver, username, password, serverdir, storedir, connstr):
        self.ftpserver = ftpserver
        self.username = username
        self.password = password
        self.serverdir = serverdir
        self.storedir = storedir
        self.ftpconn = None
        self.conn = psycopg2.connect(connstr)
        self.conn.autocommit = False
        self.prepare_statements()
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

    
    def prepare_statements(self):
        """Prepare SQL statements for use in the decoding process."""
        self.check_query = "SELECT COUNT(*) FROM session_data WHERE profilenumber = %s;"
        self.insert_query = """
        INSERT INTO session_data (sessionid, devicename, profilenumber, startdatetime, airtemp, location, filename, windspeed, winddirection, airpressure)
        VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s);
        """
        self.insert_timeseries_query = """
        INSERT INTO raw_timeseries (sessionid, seq, salinity, temperature, pressure_dbar, oxygen, fluorescens, turbidity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

    def decodeall(self):
        """
        Attempt to decode all documents and store them to PostgreSQL
        """
        for entry in os.listdir(self.storedir):
            full_path = os.path.join(self.storedir, entry)
            if os.path.isfile(full_path) and entry[0] != '.':
                try:
                    mydive = Decoder(self.storedir, entry)
                    if mydive.verifydata():
                        mydive.decode()
                        if "profilenumber" in mydive.datadict:
                            with self.conn.cursor() as cursor:
                                # Sjekk om 'profilenumber' allerede finnes i databasen
                                cursor.execute(self.check_query, (mydive.datadict["profilenumber"],))
                                count = cursor.fetchone()[0]
                                if count == 0:
                                    # Sett inn dataene i databasen
                                    if mydive.datadict.get('location') is not None:
                                        location_wkt = f'POINT({mydive.datadict["location"]["coordinates"][0]} {mydive.datadict["location"]["coordinates"][1]})'
                                    else:
                                        location_wkt = None
                                    cursor.execute(self.insert_query, (
                                        mydive.datadict['sessionid'],
                                        mydive.datadict['devicename'],
                                        mydive.datadict['profilenumber'],
                                        mydive.datadict['startdatetime'],
                                        mydive.datadict.get('airtemp'),
                                        location_wkt,
                                        mydive.datadict['filename'],
                                        mydive.datadict.get('windspeed'),
                                        mydive.datadict.get('winddirection'),
                                        mydive.datadict.get('airpressure')
                                    ))
                                    for data in mydive.datadict['rawtimeseries']:
                                        cursor.execute(self.insert_timeseries_query, (
                                            mydive.datadict['sessionid'],
                                            data['seq'],
                                            data.get('salt'),
                                            data.get('temp'),
                                            data.get('pressure(dBAR)'),  
                                            data.get('oxygene'),
                                            data.get('fluorescens'),
                                            data.get('turbidity')
                                        ))
                                    self.conn.commit()
                                    logger.debug('Saved %s to PostgreSQL', mydive.datadict["profilenumber"])
                except Exception as e:
                    # print(traceback.format_exc())
                    logger.debug('Error decoding %s (%s)', entry, str(e))

    def close(self):
        self.conn.close()



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
