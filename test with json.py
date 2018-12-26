try:
    # if python3
    from urllib.request import urlopen
except ImportError:
    # if python2 or lower
    from urllib2 import urlopen

import json
import pymssql
import datetime
from datetime import date
from datetime import time

global t_date
global t_time



#-----Receive the content of the url and pareses the json. returnes type dict-----#
def get_parsed_json_data(json_url):
        
        resp = urlopen(json_url)
        json_data = resp.read().decode("utf-8")
        parsed_data = json.loads(json_data)
        stripped_data = parsed_data['data']
        station_data = stripped_data['stations']

        epoch_time = parsed_data['last_updated']
        global t_date
        t_date = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y%m%d')
        global t_time
        t_time = datetime.datetime.fromtimestamp(epoch_time).strftime('%H:%M:%S')
        return station_data



def push_data(station_data):

        #-----open mssql conn every update and close immediately-----#
        conn = pymssql.connect(server="195.148.183.133", user="sapngl", password="357Hj23&", database="SAP-NGL")
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

        #-----Dict to store current available bikes to pushg differences to database-----#
        dict_num_bikes_curr = {}

        #-----Dict to store previous available bikes to get difference-----#
        dict_num_bikes_prev = {}
    
        #-----iterating through each station data array and pushing to database-----#
        i = 0
        while i < len(station_data):
            
            print("------------")
            indexed_data = station_data[i]
            
            for key in ('station_id', 'num_bikes_available', 'num_docks_available'):
                #print(str(key) + ': ' + str(indexed_data[key]))
                dict_data[key] = str(indexed_data[key])

            dict_num_bikes_curr.update( {indexed_data["station_id"]: indexed_data["num_bikes_available"]} ) #-----Update station current available bikes and add new stations if there are any-----#

            try:
                #key exists in previous
                dict_data["bikes_delta"] = dict_num_bikes_prev[indexed_data["station_id"]] - dict_num_bikes_curr[indexed_data["station_id"]]
                dict_num_bikes_prev.update({ [indexed_data["station_id"]]: dict_num_bikes_curr[ indexed_data["station_id"]] })
            except KeyError:
                #key does not exist in preivous
                dict_data["bikes_delta"] = 0
                dict_num_bikes_prev.update({ indexed_data["station_id"]: dict_num_bikes_curr[indexed_data["station_id"]] })

            query = generate_sql_query(dict_data)
            for keys,values in dict_num_bikes_prev.items():
                print(values)
            print(query)
            cursor.execute(query);
                
            i += 1

        #-----close connection-----#
        conn.commit()
        conn.close()



#-----Generate formatted sql query with atributes passed through the dictionary.
def generate_sql_query(dict_data):
        query = ("INSERT INTO STATION_STATUS_TURKU VALUES("
             "'{station_id}', "
             "{num_bikes_available}, "
             "{num_docks_available}, "
             "'ST', "
             "'{date}', "
             "'{time}', "
             "{delta}, "
			 "{id1}, "
			 "{id2});").format(
                 station_id = dict_data['station_id'],
                 num_bikes_available = dict_data['num_bikes_available'],
                 num_docks_available = dict_data['num_docks_available'],
                 date = t_date,
                 time = t_time,
                 delta = dict_data['bikes_delta'],
				 id1 = 12, 
				 id2 = 1)
    
        return query
		




#main code (core code) of script
        
url = ("https://api.nextbike.net/maps/gbfs/v1/nextbike_ft/en/station_status.json")
push_data(get_parsed_json_data(url))
