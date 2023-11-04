import pandas as pd
from datetime import datetime

def kevin_to_celcius(k):
    c =float(k)-273
    return round(c,2) 

def transforming(ti):
    data = ti.xcom_pull(task_id='extracting_weather_data')
    name = data['name']
    description = data['weather'][0]['description']
    data_1 = data['main']
    temp = kevin_to_celcius(data_1['temp'])
    feels_like = kevin_to_celcius(data_1['feels_like'])
    temp_min = kevin_to_celcius(data_1['temp_min'])
    temp_max = kevin_to_celcius(data_1['temp_max'])
    pressure = data_1['pressure']
    humidity = data_1['humidity']
    wind = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt']+data['timezone'])
    sunrise = datetime.utcfromtimestamp(data['sys']['sunrise']+data['timezone'])
    sunset = datetime.utcfromtimestamp(data['sys']['sunset']+data['timezone'])


    data = [name,description,temp,feels_like,temp_min,
            temp_max,pressure,humidity,wind,time_of_record,
            sunrise,sunset]
    schema = ['City', 'Description','Temperature(C)',
            'Feels Like(C)','Minimum Temp(C)','Maximun Temp(C)',
            'Pressure','Humidity','Wind Speed',
            'Time of record','Sunrise(Local Time)','Sunset(Local Time)']
    df = pd.DataFrame(data,schema,)

