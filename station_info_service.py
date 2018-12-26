import win32service  
import win32serviceutil  
import win32event
import servicemanager

try:
    # if python3
    from urllib.request import urlopen
except ImportError:
    # if python2 or lower
    from urllib2 import urlopen
import json
import pymssql
import datetime
import time
  
class PySvcNGL(win32serviceutil.ServiceFramework):  
    # NET START/STOP the service by the following name  
    _svc_name_ = "PySvcNGL_st-info"  
    # this text shows up as the service name in the Service  
    # Control Manager (SCM):  
    _svc_display_name_ = "Python Service - NGL (Information)"
    # this text shows up as the description in the SCM: 
    _svc_description_ = "This service captures JSON for Station_Information and pushes to SQL Server"
      
    def __init__(self, args):  
        win32serviceutil.ServiceFramework.__init__(self,args)


    #-----service core logic-----#     
    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        self.stopping = False
        self.main()
             

    # called when service is being stopped     
    def SvcStop(self):  
        #stop service
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STOPPED,
                              (self._svc_name_,''))
        self.stopping = True

    def main(self):

        self.current = 0

        while not self.stopping:
            # procedure is called every hour
            if datetime.datetime.now().hour != self.current:
                self.current = datetime.datetime.now().hour
                url = ("https://api.nextbike.net/maps/gbfs/v1/nextbike_ft/en/station_information.json")
                
                # in case somethiing goes wrong
                done = False
                while not done:
                    try:
                        self.push_data(self.get_parsed_json_data(url))
                        done = True
                    except Exception as e:
                        servicemanager.LogErrorMsg("Error: " + str(e) + ". Trying again")
                        done = False
                        
            time.sleep(600)

#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv#
#these are the def's that work with the actual core code                         #
#they have nothing to do with the service or its structure as with the Svc* def's#
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv#

    #-----Receive the content of the url and parses the json. returnes type dict-----#
    def get_parsed_json_data(self, json_url):
        err505 = True

        # try/exccept block o avoid the service from stopping when an error 505 occurs
        while err505:
            try:
                resshutdown p = urlopen(json_url)
                json_data = resp.read().decode("utf-8")
                parsed_data = json.loads(json_data)
                stripped_data = parsed_data['data']
                station_data = stripped_data['stations']
                err505 = False
            except Exception as e:
                servicemanager.LogErrorMsg("Error: " + str(e) + ". Trying again")
                err505 = True
                
        return station_data

    def push_data(self,station_data):

        #-----open mssql conn every update and close immediately-----#
        conn = pymssql.connect(server="###.###.###.###", user="######", password="#########", database="#######")
        cursor = conn.cursor()

        #-----Dict to store sql query attributes-----#
        dict_data = {
            "station_id": "",
            "name": "",
            "short_name": "",
            "lat": "",
            "lon": "",
            "region_id": "",
            "capacity": ""
            }

        i = 0   #iterating through each station data array and pushing to database------#
        while i < len(station_data):
            indexed_data = station_data[i]
            
            for key in ('station_id', 'name', 'short_name', 'lat', 'lon', 'region_id', 'capacity'):
                dict_data[key] = str(indexed_data[key]) #getting all data to dict per each station

            query = self.generate_sql_query(dict_data)
            cursor.execute(query);
                
            i += 1
        #-----close connection-----#
        conn.commit()
        conn.close()

    #-----Generate formatted sql query with atributes passed through the dictionary.
    def generate_sql_query(self,dict_data):
        query = ("IF EXISTS (SELECT * FROM STATION_INFORMATION_TURKU WHERE STATIONID='{station_id}') \n"
            "UPDATE STATION_INFORMATION_TURKU SET "
            "STATIONNAME='{name}', "
            "SHORTNAME='{short_name}', "
            "LATITUDE={lat}, "
            "LONGITUDE={lon}, "
            "REGIONID='{region_id}', "
            "CAPACITY={capacity}, "
            "MEASUREUNIT='ST' "
            "WHERE STATIONID='{station_id}' \n"
	    "ELSE \n"
            "INSERT INTO STATION_INFORMATION_TURKU VALUES ("
            "'{station_id}', "
            "'{name}', "
            "'{short_name}', "
            "{lat}, "
            "{lon}, "
            "'{region_id}', "
            "{capacity}, "
            "'ST')").format(**dict_data)            
        return query


if __name__ == '__main__':  
    win32serviceutil.HandleCommandLine(PySvcNGL) 
