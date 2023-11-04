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
                "retry_delay": timedelta(seconds=30)}



def kevin_to_celcius(k):
    c =float(k)-273
    return round(c,2) 


def transform_load(ti):     #ti = task_instance
    data = ti.xcom_pull(task_ids='extracting_weather_data')
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
  
    value = [name,description,temp,feels_like,temp_min,
            temp_max,pressure,humidity,wind,time_of_record,
            sunrise,sunset]
    schema = ['city', 'description','temperature_c',
            'feels_like_c','minimum_temp_c','maximun_temp_c',
            'pressure','humidity','wind_speed',
            'time_of_record','sunrise_local_time','sunset_local_time']
    
    columns_str = ','.join(schema)
    values_str = ','.join(["'" + str(i) + "'" for i in value])
    ti.xcom_push(key='values',value = values_str)
    
with DAG(
    dag_id="airflow_openweather",
    default_args=default_args,
    description="This is my first project with airflow",
    start_date=datetime(2023, 10, 29),
    schedule_interval="@once",
) as dag:
    task1 = HttpSensor(
        task_id="is_weather_api_ready",
        http_conn_id="OpenWeather_ID",
        endpoint="/data/2.5/weather?q=Hanoi&appid=6fb5bb999e53c1ed97b31cbcddce7071",
    )
    task2 = SimpleHttpOperator(
        task_id='extracting_weather_data',
        http_conn_id='OpenWeather_ID',
        endpoint='/data/2.5/weather?q=Hanoi&appid=6fb5bb999e53c1ed97b31cbcddce7071',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )
    task3 = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load
    )
    task4 = PostgresOperator(
        task_id='create_table_weather',
        postgres_conn_id='postgres',
        sql="""create table if not exists weather(
        city varchar(50),
        description	varchar(50),
        temperature_c float,
        feels_like_c float,
        minimum_temp float,
        maximun_temp float,
        pressure int,
        humidity int,
        wind_speed float,
        time_of_record varchar(50),
        sunrise_local_time varchar(50),
        sunset_local_time varchar(50))
        """
    )
    task5 = PostgresOperator(
        task_id = 'insert_data_to_table_weather',
        postgres_conn_id='postgres',
        sql='''insert into weather 
                values ({{ti.xcom_pull(task_ids='transform_load_weather_data', key='values')}})'''
         )   
    
    task1 >> task2 >> task3 >> task4 >> task5
