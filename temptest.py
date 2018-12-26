import pymssql

conn = pymssql.connect(server="195.148.183.133", user="sapngl", password="357Hj23&", database="SAP-NGL")
cursor = conn.cursor()

cursor.execute("INSERT INTO STATION_STATUS_TURKU(STATIONID, NUM_AVAILABLE_BIKES, NUM_AVAILABLE_DOCKS, MEASUREUNIT, POLL_DATE, POLL_TIME) VALUES(7081269, 8, 12, 'ST', '2018-11-21', '09:42:00.0');")
conn.commit()
conn.close()
