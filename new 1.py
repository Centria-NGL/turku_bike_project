try:
    # if python3
    from urllib.request import urlopen
except ImportError:
    # if python2 or lower
    from urllib2 import urlopen
import json
import pymssql
import time

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


def generate_sql_query(dict_data):
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


#main code (core code) of script
        
url = ("https://api.nextbike.net/maps/gbfs/v1/nextbike_ft/en/station_information.json")
push_data(get_parsed_json_data(url))
