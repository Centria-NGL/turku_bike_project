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
import datetime
import time

global t_date
global t_time
global timezone

def create_dicts(self):
    #-----Dict to store current available bikes for all stations-----#
    global dict_num_bikes_curr
    #-----Dict to store previous available bikes for all stations-----#
    global dict_num_bikes_prev

    if 'dict_num_bikes_curr' not in globals():
        dict_num_bikes_curr = {}
    if 'dict_num_bikes_prev' not in globals():
        dict_num_bikes_prev = {}

def get_parsed_json_data(json_url):
        
        resp = urlopen(json_url)
        json_data = resp.read().decode("utf-8")
        parsed_data = json.loads(json_data)
        stripped_data = parsed_data['currently']

        epoch_time = stripped_data['time']
        global t_date
        t_date = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y%m%d')
        global t_time
        t_time = datetime.datetime.fromtimestamp(epoch_time).strftime('%H:%M:%S')
        global timezone
        timezone = parsed_data['timezone']

        #for key in stripped_data:
            #print(str(key) + ": " + str(stripped_data[key]))

        return stripped_data


def push_db(data):

    dict_data = {
            "date": t_date,
            "time": t_time,
            "timezone": timezone#,
            #"summary": "",
            #"precipProbability": "",
            #"precipIntensity": "",
            #"precipType": "sleet",
            #"precipAccumulation": "",
            #"temperature": "",
            #"apparentTemperature": "",
            #"humidity": "",
            #"pressure": "",
            #"windSpeed": "",
            #"cloudCover": "",
            #"uvIndex": ""
            }

    conn = pymssql.connect(server="195.148.183.133", user="sapngl", password="357Hj23&", database="SAP-NGL")
    cursor = conn.cursor()

    for key in (list(data)):
                #print(str(key) + ': ' + str(indexed_data[key]))
        if key != 'time':
            if key == 'cloudCover':
                print(str(round(data.get(key) * 8)))
                dict_data.update({str(key) : str(round(data.get(key) * 8))})
            elif key == 'humidity' or key is 'precipProbaility':
                dict_data.update({str(key) : str(round(data[key] * 100))})
            else:
                dict_data.update({str(key):str(data[key])})

    if 'precipAccumulation' not in list(data):
        dict_data.update({'precipAccumulation': "NULL"})
        
    if 'precipType' not in list(data):
        dict_data.update({'precipType': "NULL"})
    else:
        string = "'" + str(data['precipType']) + "'"
        dict_data.update({'precipType': string})



    query = generate_sql_query(dict_data) 
    print(query)
    cursor.execute(query);
    conn.commit();
    conn.close();



def generate_sql_query(dict_data):
    #for key in dict_data:
            #print(str(key) + ": " + str(dict_data[key]))
    query = ("INSERT INTO FORECAST_WEATHER_TURKU VALUES("
            "'{date}', "
            "'{time}', "
            "'{timezone}', "
            "'{summary}', "
            "{precipProbability}, "
            "{precipIntensity}, "
            "{precipType}, "
            "{precipAccumulation}, "
            "{temperature}, "
            "{apparentTemperature}, "
            "{humidity}, "
            "{pressure}, "
            "{windSpeed}, "
            "{cloudCover}, "
            "{uvIndex});").format(**dict_data)

    return query


url = ("https://api.darksky.net/forecast/9d8bc347f11f7a116c5f8f3d42681745/60.4518,22.2666?exclude=minutely,daily,alerts,flags&units=si")
push_db(get_parsed_json_data(url))
