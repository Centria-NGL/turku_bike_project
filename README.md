# Turku Bike Project

This repo contains the python installable scripts used for data-capture and push to a MSSQL Remote Server Database.

These scripts are installable in the Windows operating system as Windows Services.

### *station_info_service.py*:

This script runs as a Windows Service to capture data from the Next-Bike station information API.

The API provides an array of all stations, including information like, location, capacity, identifier, etc. The reply is in JSON format, therefore, the script manages this reply, and formats the SQL query to include the data provided by the API itself.

The service is coded so, to refresh every hour, and update the corresponding table on the remote MSSQL Server.

### *station_stat_service*:

This script runs as a Windows Service to capture data from the Next-Bike station status API.

The API provides an array of all station's status information, including current available bikes, current free docks, etc. The reply is in JSON format, therefore, the script manages this reply, and formats the SQL query to include the data provided by the API itself.

The service is coded so, to refresh every 50 seconds, and append the current reading to the respective table on the MSSQL Server.

### *current_weather_service*:

This script runs as a Windows Service to capture weather data using API calls to the DarkSky weather provider.

This API allows customization of the reply by modifing the calling URL. The reply customization is done as per the [DarkSky Developer's Documentation](https://darksky.net/dev/docs "DarkSky API Documentation").

The API's response is in JSON format, and is processed within the script to match requirements as needed. The data is pushed to the respective table on the MSSQL Server