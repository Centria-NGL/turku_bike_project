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

global t_date
global t_time
  
class PySvcNGL(win32serviceutil.ServiceFramework):  
    # you can NET START/STOP the service by the following name  
    _svc_name_ = "PySvcNGL_st-stat"  
    # this text shows up as the service name in the Service  
    # Control Manager (SCM)  
    _svc_display_name_ = "Python Service - NGL (Status)"
    # this text shows up as the description in the SCM  
    _svc_description_ = "This service captures JSON for Station_Status and pushes to SQL Server"
      
    def __init__(self, args):  
        win32serviceutil.ServiceFramework.__init__(self,args)


    #-----service core logic-----#     
    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_,''))
        self.stopping = False
        self.main()
             

    # called when service is being shutdown      
    def SvcStop(self):  
        #stop service
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STOPPED,
                              (self._svc_name_,''))
        self.stopping = True

    def main(self):
        self.create_dicts()
        self.current = 0

        
        while not self.stopping:
            if datetime.datetime.now().minute != self.current:
                self.current = datetime.datetime.now().minute
                url = ("https://api.nextbike.net/maps/gbfs/v1/nextbike_ft/en/station_status.json")
                
                # in case somethiing goes wrong
                done = False
                while not done:
                    try:
                        self.push_data(self.get_parsed_json_data(url))
                        done = True
                    except Exception as e:
                        servicemanager.LogErrorMsg("Push error: " + str(e) + ". Trying again")
                        done = False
            time.sleep(10)

#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv#
#these are the def's that work with the actual core code                         #
#they have nothing to do with the service or its structure as with the Svc* def's#
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv#

    def create_dicts(self):
        #-----Dict to store current available bikes for all stations-----#
        global dict_num_bikes_curr
        #-----Dict to store previous available bikes for all stations-----#
        global dict_num_bikes_prev

        if 'dict_num_bikes_curr' not in globals():
            dict_num_bikes_curr = {}
        if 'dict_num_bikes_prev' not in globals():
            dict_num_bikes_prev = {}


        
    #-----Receive the content of the url and parses the json. returnes type dict-----#
    def get_parsed_json_data(self, json_url):
        err505 = True

        # try/exccept block o avoid the service from stopping when an error 505 occurs
        while err505:
            try:
                resp = urlopen(json_url)
                json_data = resp.read().decode("utf-8")
                parsed_data = json.loads(json_data)
                stripped_data = parsed_data['data']
                station_data = stripped_data['stations']
                err505 = False
            except Exception as e:
                servicemanager.LogErrorMsg("JSON retrieve error: " + str(e) + ". Trying again")
                err505 = True

        epoch_time = parsed_data['last_updated']
        global t_date
        t_date = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y%m%d')
        global t_time
        t_time = datetime.datetime.fromtimestamp(epoch_time).strftime('%H:%M:%S')
        return station_data

    def push_data(self, station_data):
        global dict_num_bikes_curr    
        global dict_num_bikes_prev    
        
        

        #-----open mssql conn every update and close immediately-----#
        conn = pymssql.connect(server="###.###.###.###", user="######", password="########", database="#######")
        cursor = conn.cursor()

        #-----Dict to store sql query attributes-----#
        dict_data = {
            "station_id": "",
            "num_bikes_available": "",
            "num_docks_available": "",
            "t_date": t_date,
            "t_time": t_time,
            "bikes_delta": ""
            }
        i = 0   #iterating through each station data array and pushing to database
        while i < len(station_data):
            indexed_data = station_data[i]
            
            for key in ('station_id', 'num_bikes_available', 'num_docks_available'):
                dict_data[key] = str(indexed_data[key]) #getting all data to dict per each station

            dict_num_bikes_curr.update( {dict_data["station_id"]: dict_data["num_bikes_available"]} ) #Update station current available bikes and add new stations if there are any

            if dict_data["station_id"] in dict_num_bikes_prev.keys():
                #key exists in previous
                dict_data["bikes_delta"] = (int(dict_num_bikes_prev.get(dict_data["station_id"])) - int(dict_num_bikes_curr.get(dict_data["station_id"])))
                dict_num_bikes_prev.update({ dict_data["station_id"]: dict_num_bikes_curr.get(dict_data["station_id"]) })
            else:
                #key does not exist in preivous
                dict_data["bikes_delta"] = 0
                dict_num_bikes_prev.update({ dict_data["station_id"]: dict_num_bikes_curr[dict_data["station_id"]] })

            query = self.generate_sql_query(dict_data)
            cursor.execute(query);
   
            i += 1

        #-----close connection-----#
        conn.commit()
        conn.close()

    #-----Generate formatted sql query with atributes passed through the dictionary.
    def generate_sql_query(self, dict_data):
        query = ("INSERT INTO STATION_STATUS_TURKU VALUES("
             "'{station_id}', "
             "{num_bikes_available}, "
             "{num_docks_available}, "
             "'ST', "
             "'{date}', "
             "'{time}', "
             "{delta});").format(
                 station_id = dict_data['station_id'],
                 num_bikes_available = dict_data['num_bikes_available'],
                 num_docks_available = dict_data['num_docks_available'],
                 date = t_date,
                 time = t_time,
                 delta = dict_data['bikes_delta'])
    
        return query


if __name__ == '__main__':  
    win32serviceutil.HandleCommandLine(PySvcNGL) 
