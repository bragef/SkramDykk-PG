"""
(c) Nils Jacob Berland 2016

"""

import arrow
import uuid
from datetime import datetime

def filename2date(filename):
    yyyy = '20'+filename[:2]
    mm = filename[2:4]
    dd = filename[4:6]
    return arrow.get(yyyy+'-'+mm+'-'+dd,'YYYY-MM-DD')

def geolocstring(gpsstr):
    try:
        (lat,lon) = gpsstr[5:].split(" ")
        declat = float(lat[1:])/100
        declon = float(lon[1:])/100
        return {"type":"Point", "coordinates":[declat, declon]}
    except:
        return {}

def isfloat(s):
    try:
        r = float(s)
    except:
        return False
    return True

class decoder(object):
    def __init__(self, path, filename ):
        self.path = path
        self.filename = filename

        try:
            fp = open(path+filename,"r")
            datastr=fp.read()
            if len(datastr)>0:
                self.datastr = datastr
            else:
                self.datastr = ""
            #print(self.datastr)
        except:
            self.datastr = ""
            print("Could not open ", filename)

    def verifydata(self):
        if self.datastr == "":
            return False
        try:
            lines = str(self.datastr).strip().split('\n')
            if lines[-1].find("End of data")==0 and lines[0][0] == '#':
                return True
            else:
                return False
        except:
            return False

    def decode(self):
        """
        decode will create a dictionary with data from the file
        if everything is OK we return a populated json document
        otherwise we return an empty document
        """
        self.datadict = {}
        lines = str(self.datastr).strip().split('\n')

        # do some simple verifications
        # test that first 5 lines start with # and that the last line has "End of data"
        try:
            #print(lines[-1])
            if lines[0][0]=="#" and lines[4][0]=='#' and lines[-1].find("End of data")==0:
                pass
                # print("valid")
            else:
                # print("invalid")
                return {}
            (r_devicename,r_netstat,r_profileid) = lines[0][1:].split(',')
            (r_depth, r_mode, r_speed ) = lines[1][1:].split(',')
            (r_depth, r_starttime, r_finish, r_nextdate, r_nexttime) = lines[2][1:].split(',')
            (r_mtilt, r_xtilt, r_ytilt, r_gpsloc, r_temp, r_pressure, r_waterdensity, r_windspeed ) = lines[4][1:].split(',')
            self.extra_info1 = lines[5]
            self.extra_info2 = lines[6]
            #print(r_devicename,r_netstat, r_profileid, r_depth, r_mode, r_speed, r_windspeed)
            #print(self.extra_info1)
            #print(self.extra_info2)
        except:
            return {}

        # pull out data from the first lin

        # find the date
        # this is an ugly hack to find the dive datetime! SAIVAS need to fix their clock!
        starttime = r_starttime.split(':')[1].replace('.',':').split('+')[0].strip()
        # find the date from the filename!!! this is just sooo stupid!!!
        yyyy = '20'+self.filename[:2]
        mm = self.filename[2:4]
        dd = self.filename[4:6]
        ds = str(yyyy+'-'+mm+'-'+dd+"T"+starttime)
        #print(ds)

        self.divedatetime = datetime.strptime(ds,'%Y-%m-%dT%H:%M:%S')
        self.datadict['sessionid'] = uuid.uuid4()
        self.datadict['devicename'] = r_devicename
        self.datadict['profilenumber'] = int(r_profileid.split(':')[1])
        self.datadict['startdatetime'] = self.divedatetime
        airtemp_string = r_temp.split(':')[1][:-1]
        if isfloat(airtemp_string):
            self.datadict['airtemp'] = float(airtemp_string)
        self.datadict['location'] = geolocstring(r_gpsloc)
        self.datadict['filename'] = self.filename

        # find all the datalines
        timeseries = []
        for line in lines[7:]:
            if line[0] == 'N':
                # print(line)
                payload = {}
                itemlist = line.split()
                # iterate over all items and choose the ones we understand/support
                # N00012 S31.156 T+07.288 P0011.97 OX054.22 OF000.13 OT000.07
                for item in itemlist:
                    if item[0] == 'N':
                        payload['seq'] = int(item[1:])
                    elif item[0] == 'S':
                        payload['salt'] = float(item[1:])
                    elif item[0] == 'T':
                        payload["temp"] = float(item[1:])
                    elif item[0] == 'P':
                        payload['pressure(dBAR)'] = float(item[1:])
                    elif item[:2] == 'OX':
                        payload["oxygene"] = float(item[2:])
                    elif item[:2] == 'OF':
                        payload["fluorescens"] = float(item[2:])
                    elif item[:2] == 'OT':
                        payload["turbidity"] = float(item[2:])
                    else:
                        pass
                        #print("Unknown item ", item)
                timeseries.append(payload)
        self.datadict['rawtimeseries'] = timeseries


if __name__ == "__main__":

    filename = "15082603.txt"
    path = "/Users/njberland/PycharmProjects/amalieskram/textfiles/"

    mydive = decoder(path,filename)
    if mydive.verifydata():
        mydive.decode()
        print(mydive.datadict)
    else:
        print("Just crap")