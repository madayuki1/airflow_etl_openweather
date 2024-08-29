from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys
import os
import requests
import pandas as pd
import pendulum

local_tz = pendulum.timezone('Asia/Singapore')

APP_ID = os.getenv("OPENWEATHER_API_KEY")

with DAG(
    dag_id = 'open_weather_api_dag',
    start_date=datetime(2024, 8, 29, 15, 8, 0, tzinfo=local_tz),
    schedule_interval="*/2 * * * *",  # Run every 2 minutes
    catchup=False,
    tags=['open_weather']
) as dag:
    @task()
    def extract():
        params = {
                "appid": APP_ID,
                "q" : "Jakarta",
                "units": "metric"
            }
        
        url = "https://api.openweathermap.org/data/2.5/weather"
        response = requests.get(url=url, params=params)

        if (response.status_code == 200):
            data = response.json()
            weather =  data["weather"][0]['main']
            temp = data['main']['temp']
            temp_feels_like = data['main']['feels_like']
            pressure = data['main']['pressure']
            visibility = data['visibility']
            humidity = data['main']['humidity']
            wind_speed = data['wind']['speed']
            city = data['name']

            # Combine extracted fields into a dictionary
            combined_data = {
                'city': city,
                'weather': weather,
                'temperature': temp,
                'temperature_feels_like': temp_feels_like,
                'wind_speed': wind_speed,
                'pressure': pressure,
                'visibility': visibility,
                'humidity': humidity
            }

            df = pd.DataFrame([combined_data])
            return df.to_dict()
        else:
            return {
                'status': 'failed',
                'reason': f'API request failed with status code {response.status_code}'
            }

    @task()
    def transform(data):
        if isinstance(data, dict) and data.get('status') == 'failed':
            return data
        
        df = pd.DataFrame.from_dict(data)
        df['Event Suitability'] = df.apply(lambda row: 'Suitable' if row['weather'] not in ['thunderstorm', 'Drizzle', 'Rain', 'Snow'] and 15 <= row['temperature'] <= 35 else 'Unsuitable', axis=1)
        df['Safety Warning'] = df.apply(lambda row: 'Warning: High Winds' if row['wind_speed']  > 10 else ('Warning: Low Visibility' if row['visibility'] < 1000 else 'No Warning'), axis=1)
        df['Comfort Level'] = df.apply(lambda row: 'Comfortable' if 18 <= row['temperature'] <= 33 and row['humidity'] < 60 else 'uncomfortable', axis=1)

        return df.to_dict()
    
    @task()
    def load(data):
        from sqlalchemy import create_engine
        import sqlite3

        if isinstance(data, dict) and data.get('status') == 'failed':
            return data

        engine = create_engine('postgresql+psycopg2://postgres:postgres@postgres-data/postgres')
        conn = engine.raw_connection()

        df = pd.DataFrame.from_dict(data)

        df['created_date'] = datetime.now()

        df.to_sql(name='weather', con=engine, if_exists='append', index=False)
    
    extract_data = extract()
    transform_data = transform(extract_data)
    load_data = load(transform_data)

    extract_data >> transform_data >> load_data

