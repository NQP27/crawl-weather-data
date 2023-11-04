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
    with DAG(
        dag_id="forecast_daily_16",
        default_args=default_args,
        description="Forecast weather next 16 days in Hanoi",
        start_date=datetime(2023, 11, 3),
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
            endpoint='/data/2.5/forecast/daily?lat=21.0245&lon=105.8412&cnt=16&appid=6fb5bb999e53c1ed97b31cbcddce7071',
            method='GET',
            response_filter=lambda r: json.loads(r.text),
            log_response=True
        )
        # task3 = PythonOperator(
        #     task_id='transform_weather_data',
        #     python_callable=transform_load
        # )
        # task4 = PostgresOperator(
        #     task_id='create_table_weather',
        #     postgres_conn_id='postgres',
        #     sql="""create table if not exists weather(
        #     city varchar(50),
        #     description	varchar(50),
        #     temperature_c float,
        #     feels_like_c float,
        #     minimum_temp float,
        #     maximun_temp float,
        #     pressure int,
        #     humidity int,
        #     wind_speed float,
        #     time_of_record varchar(50),
        #     sunrise_local_time varchar(50),
        #     sunset_local_time varchar(50))
        #     """
        # )
        # task5 = PostgresOperator(
        #     task_id = 'insert_data_to_table_weather',
        #     postgres_conn_id='postgres',
        #     sql='''insert into weather 
        #             values ({{ti.xcom_pull(task_ids='transform_load_weather_data', key='values')}})'''
        #     )   
        
        task1 >> task2 
        
