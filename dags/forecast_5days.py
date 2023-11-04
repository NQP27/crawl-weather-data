from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG
import pandas as pd
import json
import csv


default_args = {"owner": "Phuoc",
                "retries": 0,
                "retry_delay": timedelta(seconds=120)}



def kevin_to_celcius(k):
    c =float(k)-273
    return round(c,2) 


def transform_load_data(ti):     #ti = task_instance
    data = ti.xcom_pull(task_ids='extracting_data')
    lst=data['list']
    
    now = datetime.now() +timedelta(hours=7)
    now = now.strftime("%d-%m-%Y-%H-%M")
    city_name = data['city']['name'] 
    
    local_time=[]
    temp=[]
    feels_like=[]
    humidity=[]
    weather_description=[]
    cloudiness=[]
    wind_speed=[]
    rain_in_last_3h =[]
    pop =[]
    day_night =[]
    snow_in_last_3h=[]
    timezone = data['city']['timezone']
    for i in lst:
        #utcfromtimestamp la ham lay gio quoc te (gio Anh)
        local_time.append(datetime.utcfromtimestamp(i['dt'] + timezone))
        temp.append(kevin_to_celcius(i['main']['temp']))
        feels_like.append(kevin_to_celcius(i['main']['feels_like']))
        humidity.append(i['main']['humidity'])
        weather_description.append(i['weather'][0]['description'])
        cloudiness.append(i['clouds']['all'])
        wind_speed.append(i['wind']['speed'])
        if 'rain' in i:
            rain_in_last_3h.append(i['rain']['3h'])
        else: 
            rain_in_last_3h.append(0)
        if 'snow' in i:
                snow_in_last_3h.append(i['snow']['3h'])
        else: 
            snow_in_last_3h.append(0)
        pop.append(i['pop'])
        day_night.append(i['sys']['pod'])
        data ={'local_time':local_time,\
        'temp(C)':temp,\
        'feels_like(C)':feels_like,\
        'humidity(%)':humidity,\
        'weather_description':weather_description,\
        'cloudiness(%)':cloudiness,\
        'wind_speed(kmph)':wind_speed,\
        'rain_in_last_3h(mm)':rain_in_last_3h,\
        'snow_in_last_3h(mm)':snow_in_last_3h,\
        'pop(%)(ti_le_co_mua)':pop,\
        'day_night':day_night    
        }
        df = pd.DataFrame(data)
        df.to_csv(f'/opt/airflow/output/weather-forecast-in-{city_name}-extracted_at_{now}.csv')
               
    
with DAG(
    dag_id="forecast_weather_Hanoi",
    default_args=default_args,
    description="Forecast next next 5 days/3hrs in Hanoi",
    start_date=datetime(2023, 11, 4),
    schedule_interval="* */5 * * *",
) as dag:
    task1 = HttpSensor(
        task_id="is_weather_api_ready",
        http_conn_id="OpenWeather_ID",
        endpoint="/data/2.5/forecast?q=Hanoi&appid=00cbd97d8416679561d78184ae7d1a08",
    )
    task2 = SimpleHttpOperator(
        task_id='extracting_data',
        http_conn_id='OpenWeather_ID',
        endpoint="/data/2.5/forecast?q=Hanoi&appid=00cbd97d8416679561d78184ae7d1a08",
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )
    task3 = PythonOperator(
        task_id ='transform_load_data',
        python_callable=transform_load_data
    )
    task1 >> task2 >> task3
