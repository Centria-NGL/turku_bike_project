import win32service  
import win32serviceutil  
import win32event
try:
    # if python3
    from urllib.request import urlopen
except ImportError:
    # if python2 or lower
    from urllib2 import urlopen
import json
import pymssql
import time
  
class PySvcNGL(win32serviceutil.ServiceFramework):  
    # you can NET START/STOP the service by the following name  
    _svc_name_ = "PySvcNGL"  
    # this text shows up as the service name in the Service  
    # Control Manager (SCM)  
    _svc_display_name_ = "Python Service - NGL"  
    # this text shows up as the description in the SCM  
    _svc_description_ = "This service captures JSON and pushes to SQL Server"  
      
    def __init__(self, args):  
        win32serviceutil.ServiceFramework.__init__(self,args)


    #-----core logic of the service-----#     
    def SvcDoRun(self):
        self.stopping = False
        sql_ops = SqlOps()

        current = 0

        while not self.stopping:
            time.sleep(10)
            if datetime.datetime.now().minute != current:
                current = datetime.datetime.now().minute
                url = ("https://api.nextbike.net/maps/gbfs/v1/nextbike_ft/en/station_information.json")
                sql_ops.push_data(sql_ops.get_parsed_json_data(url))
    #-----------------------------------#     

    # called when service is being shutdown      
    def SvcStop(self):  
        #stop service
        self.stopping = True



#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv#
#these are the def's that work with the actual core code                         #
#they have nothing to do with the service or its structure as with the Svc* def's#
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv#

        
class SqlOps:
    #-----Receive the content of the url and parses the json. returnes type dict-----#
    def get_parsed_json_data(json_url):
        
        resp = urlopen(json_url)
        json_data = resp.read().decode("utf-8")
        return json.loads(json_data)

    def push_data(parsed_data):

        #-----open mssql conn every update and close immediately-----#
        conn = pymssql.connect(server="195.148.183.133", user="sapngl", password="357Hj23&", database="SAP-NGL")
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

        #------get data only------#
        unused_data = parsed_data['data'] #data without the json overhead brackets
        station_data = unused_data['stations'] #usable data, formatted
        
        #------iterating through each station data array and pushing to database------#
        i = 0
        while i < len(station_data):
            
            print("------------")
            indexed_data = station_data[i]
            
            for key in ('station_id', 'name', 'short_name', 'lat', 'lon', 'region_id', 'capacity'):
                print(str(key) + ': ' + str(indexed_data[key]))
                dict_data[key] = str(indexed_data[key])

            query = generate_sql_query(dict_data)
            cursor.execute(query);
                
            i += 1
        #-----close connection-----#
        conn.commit()
        conn.close()

    #-----Generate formatted sql query with atributes passed through the dictionary.
    def generate_sql_query(dict_data):
        query = ("UPDATE STATION_INFORMATION_TURKU SET "
                 "STATIONNAME='{name}', "
                 "SHORTNAME='{short_name}', "
                 "LATITUDE={lat}, "
                 "LONGITUDE={lon}, "
                 "REGIONID='{region_id}', "
                 "CAPACITY={capacity}, "
                 "MEASUREUNIT='ST' "
                 "WHERE STATIONID = '{station_id}'").format(**dict_data)
            
        return query











if __name__ == '__main__':  
    win32serviceutil.HandleCommandLine(PySvcNGL) 
