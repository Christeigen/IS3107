from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
import sqlite3
import requests
from supabase import create_client

default_args = {
    "owner": "airflow",
    "retries": 3
}

with DAG(
    dag_id="traffic_images_collector",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["traffic_data"],
    description="Collect traffic images and save them in sqlite3"
) as dag:

    @task
    def get_api_key():
        conn_id = "lta_api_connection"
        connection = BaseHook.get_connection(conn_id)
        return connection.extra_dejson.get("AccountKey", None)

    @task
    def fetch_traffic_data(api_key: str):
        if not api_key:
            raise Exception("API key not found in Airflow connection extras")
        base_url = "https://datamall2.mytransport.sg/ltaodataservice/Traffic-Imagesv2"
        headers = {"AccountKey": api_key, "accept": "application/json"}
        response = requests.get(base_url, headers=headers)
        if response.status_code == 200:
            return response.json()["value"]
        else:
            raise Exception(f"Failed to fetch traffic images data: {response.status_code}")

    @task
    def upsert_data(traffic_data: list):
        connection = BaseHook.get_connection("lta_api_connection")
        url_supabase = connection.extra_dejson.get("SUPABASE_URL", None)
        key = connection.extra_dejson.get("SUPABASE_KEY", None)
        supabase = create_client(url_supabase, key)
        supabase.table("TrafficImages").upsert(traffic_data).execute()

    upsert_data(fetch_traffic_data(get_api_key()))