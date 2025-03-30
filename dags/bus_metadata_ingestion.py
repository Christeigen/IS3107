from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import requests
from airflow.hooks.base_hook import BaseHook  # Import BaseHook
import sqlite3  # Use sqlite3 instead of psycopg2
import json
import zipfile
import io
import csv
import os

# Function to create the database schema for SQLite (tables with foreign keys)
def create_db_schema(**kwargs):
    # Connect to your SQLite database (make sure to replace with your SQLite file path)
    conn = sqlite3.connect('/home/houss/airflow/bus.db')   # SQLite file-based DB
    cursor = conn.cursor()

    # Create BusStop table (SQLite syntax)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BusStop (
            BusStopCode TEXT PRIMARY KEY,
            RoadName TEXT,
            Description TEXT,
            Latitude REAL,
            Longitude REAL
        );
    """)

    # Create BusService table (SQLite syntax)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BusService (
            ServiceNo TEXT,
            Operator TEXT,
            Direction TEXT,
            Category TEXT,
            OriginCode TEXT,
            DestinationCode TEXT,
            AM_Peak_Freq INTEGER,
            AM_Offpeak_Freq INTEGER,
            PM_Peak_Freq INTEGER,
            PM_Offpeak_Freq INTEGER,
            LoopDesc TEXT,
            FOREIGN KEY (OriginCode) REFERENCES BusStop(BusStopCode),
            FOREIGN KEY (DestinationCode) REFERENCES BusStop(BusStopCode),
            PRIMARY KEY (ServiceNo, Direction)
        );
    """)

    # Create BusRoute table (SQLite syntax)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS BusRoute (
            ServiceNo TEXT,
            StopSequence INTEGER,
            BusStopCode TEXT,
            Distance REAL,
            Direction TEXT,
            FOREIGN KEY (ServiceNo) REFERENCES BusService(ServiceNo),
            FOREIGN KEY (BusStopCode) REFERENCES BusStop(BusStopCode),
            PRIMARY KEY (ServiceNo, Direction, StopSequence)
        );
    """)

    # Create PassengerVolume table for tap in/tap out data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS PassengerVolume (
            YearMonth TEXT,
            DayType TEXT,
            TimePerHour INTEGER,
            BusStopCode TEXT,
            TapInVolume INTEGER,
            TapOutVolume INTEGER,
            PRIMARY KEY (YearMonth, DayType, TimePerHour, BusStopCode),
            FOREIGN KEY (BusStopCode) REFERENCES BusStop(BusStopCode)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

# Function to get the API key from Airflow Connections
def get_api_key(**kwargs):
    conn_id = "lta_api_connection"
    connection = BaseHook.get_connection(conn_id)
    return connection.extra_dejson.get("AccountKey", None)

# Function to fetch Bus Stops data with pagination
def fetch_bus_stops_data(**kwargs):
    api_key = get_api_key(**kwargs)  # Fetch the API key from Airflow connection
    if api_key is None:
        raise Exception("API key not found in Airflow connection extras")
    base_url = "http://datamall2.mytransport.sg/ltaodataservice/BusStops"
    headers = {"AccountKey": api_key, "accept": "application/json"}
    bus_stops = []
    skip = 0
    while True:
        url = f"{base_url}?$skip={skip}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("value", [])
            bus_stops.extend(data)
            if len(data) < 500:
                break
            else:
                skip += 500
        else:
            raise Exception(f"Failed to fetch bus stops data: {response.status_code}")
    return bus_stops

# Function to fetch Bus Routes data with pagination
def fetch_bus_routes_data(**kwargs):
    api_key = get_api_key(**kwargs)
    if api_key is None:
        raise Exception("API key not found in Airflow connection extras")
    base_url = "http://datamall2.mytransport.sg/ltaodataservice/BusRoutes"
    headers = {"AccountKey": api_key, "accept": "application/json"}
    bus_routes = []
    skip = 0
    while True:
        url = f"{base_url}?$skip={skip}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("value", [])
            bus_routes.extend(data)
            if len(data) < 500:
                break
            else:
                skip += 500
        else:
            raise Exception(f"Failed to fetch bus routes data: {response.status_code}")
    return bus_routes

# Function to fetch Bus Services data with pagination
def fetch_bus_services_data(**kwargs):
    api_key = get_api_key(**kwargs)
    if api_key is None:
        raise Exception("API key not found in Airflow connection extras")
    base_url = "http://datamall2.mytransport.sg/ltaodataservice/BusServices"
    headers = {"AccountKey": api_key, "accept": "application/json"}
    bus_services = []
    skip = 0
    while True:
        url = f"{base_url}?$skip={skip}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("value", [])
            bus_services.extend(data)
            if len(data) < 500:
                break
            else:
                skip += 500
        else:
            raise Exception(f"Failed to fetch bus services data: {response.status_code}")
    return bus_services

# Function to fetch Passenger Volume data (tap in/tap out) for bus stops
def fetch_passenger_volume_data(**kwargs):
    api_key = get_api_key(**kwargs)
    if api_key is None:
        raise Exception("API key not found in Airflow connection extras")
    url = "http://datamall2.mytransport.sg/ltaodataservice/PV/Bus"
    headers = {"AccountKey": api_key, "accept": "application/json"}
    params = {"Date": "202501"}  # Example: January 2025
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        download_link = data.get("value")[0].get("Link")
        zip_response = requests.get(download_link)
        if zip_response.status_code == 200:
            os.makedirs("PassengerVolumeData", exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
                z.extractall("PassengerVolumeData")
            print("Passenger volume data downloaded and extracted successfully.")
        else:
            raise Exception(f"Failed to download ZIP file: {zip_response.status_code}")
    else:
        raise Exception(f"Error fetching passenger volume data: {response.status_code} - {response.text}")

# Function to transform and load Bus Stops into SQLite
def transform_and_load_bus_stops(**kwargs):
    bus_stops_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_bus_stops')
    conn = sqlite3.connect('/home/houss/airflow/bus.db')
    cursor = conn.cursor()
    for stop in bus_stops_data:
        cursor.execute("""
            INSERT OR REPLACE INTO BusStop (BusStopCode, RoadName, Description, Latitude, Longitude)
            VALUES (?, ?, ?, ?, ?)
        """, (stop['BusStopCode'], stop['RoadName'], stop['Description'], stop['Latitude'], stop['Longitude']))
    conn.commit()
    cursor.close()
    conn.close()

# Function to transform and load Bus Services into SQLite
def transform_and_load_bus_services(**kwargs):
    bus_services_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_bus_services')
    conn = sqlite3.connect('/home/houss/airflow/bus.db')
    cursor = conn.cursor()
    for service in bus_services_data:
        cursor.execute("""
            INSERT OR REPLACE INTO BusService (ServiceNo, Operator, Direction, Category, OriginCode, DestinationCode, AM_Peak_Freq, AM_Offpeak_Freq, PM_Peak_Freq, PM_Offpeak_Freq, LoopDesc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (service['ServiceNo'], service['Operator'], service['Direction'], service['Category'], 
              service['OriginCode'], service['DestinationCode'], service['AM_Peak_Freq'], service['AM_Offpeak_Freq'],
              service['PM_Peak_Freq'], service['PM_Offpeak_Freq'], service['LoopDesc']))
    conn.commit()
    cursor.close()
    conn.close()

# Function to transform and load Bus Routes into SQLite
def transform_and_load_bus_routes(**kwargs):
    bus_routes_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_bus_routes')
    conn = sqlite3.connect('/home/houss/airflow/bus.db')
    cursor = conn.cursor()
    for route in bus_routes_data:
        cursor.execute("""
            INSERT OR REPLACE INTO BusRoute (ServiceNo, StopSequence, BusStopCode, Distance, Direction)
            VALUES (?, ?, ?, ?, ?)
        """, (route['ServiceNo'], route['StopSequence'], route['BusStopCode'], route['Distance'], route["Direction"]))
    conn.commit()
    cursor.close()
    conn.close()

# Function to transform and load Passenger Volume data into SQLite
def transform_and_load_passenger_volume(**kwargs):
    directory = "PassengerVolumeData"
    csv_file = None
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            csv_file = os.path.join(directory, file)
            break
    if not csv_file:
        raise Exception("No CSV file found in PassengerVolumeData directory")
    
    conn = sqlite3.connect('/home/houss/airflow/bus.db')
    cursor = conn.cursor()
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header row
        for row in reader:
            # Expected row format: YEAR_MONTH, DAY_TYPE, TIME_PER_HOUR, PT_TYPE, PT_CODE, TOTAL_TAP_IN_VOLUME, TOTAL_TAP_OUT_VOLUME
            year_month, day_type, time_per_hour, pt_type, bus_stop_code, tap_in, tap_out = row
            
            # Convert numeric fields, defaulting to 0 if the field is empty.
            time_val = int(time_per_hour) if time_per_hour.strip() != "" else 0
            tap_in_val = int(tap_in) if tap_in.strip() != "" else 0
            tap_out_val = int(tap_out) if tap_out.strip() != "" else 0
            
            cursor.execute("""
                INSERT OR REPLACE INTO PassengerVolume (YearMonth, DayType, TimePerHour, BusStopCode, TapInVolume, TapOutVolume)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (year_month, day_type, time_val, bus_stop_code, tap_in_val, tap_out_val))
    conn.commit()
    cursor.close()
    conn.close()


# Function to log the metadata ingestion process
def log_metadata_ingestion(**kwargs):
    print("Metadata ingestion completed successfully!")
    conn = sqlite3.connect('/home/houss/airflow/bus.db')
    cursor = conn.cursor()
    cursor.execute("select BusStopCode from BusStop")
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    result = [dict(zip(columns, row)) for row in rows]
    json_data = json.dumps(result, indent=2)
    with open('bus_stops.json', 'w') as f:
        f.write(json_data)
    cursor.close()
    conn.close()

# DAG definition
with DAG(
    "bus_metadata_ingestion",
    default_args={
        "owner": "airflow",
        "retries": 3,
    },
    tags=["bus_metadata_ingestion"],
    description="Ingest bus metadata (routes, stops, services, and passenger volume) from API",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    create_db_schema_task = PythonOperator(
        task_id="create_db_schema",
        python_callable=create_db_schema,
        provide_context=True,
    )

    fetch_bus_stops = PythonOperator(
        task_id="fetch_bus_stops",
        python_callable=fetch_bus_stops_data,
        provide_context=True,
    )

    fetch_bus_routes = PythonOperator(
        task_id="fetch_bus_routes",
        python_callable=fetch_bus_routes_data,
        provide_context=True,
    )

    fetch_bus_services = PythonOperator(
        task_id="fetch_bus_services",
        python_callable=fetch_bus_services_data,
        provide_context=True,
    )

    fetch_passenger_volume = PythonOperator(
        task_id="fetch_passenger_volume",
        python_callable=fetch_passenger_volume_data,
        provide_context=True,
    )

    transform_and_load_stops = PythonOperator(
        task_id="transform_and_load_bus_stops",
        python_callable=transform_and_load_bus_stops,
        provide_context=True,
    )

    transform_and_load_routes = PythonOperator(
        task_id="transform_and_load_bus_routes",
        python_callable=transform_and_load_bus_routes,
        provide_context=True,
    )

    transform_and_load_services = PythonOperator(
        task_id="transform_and_load_bus_services",
        python_callable=transform_and_load_bus_services,
        provide_context=True,
    )

    transform_and_load_passenger_volume = PythonOperator(
        task_id="transform_and_load_passenger_volume",
        python_callable=transform_and_load_passenger_volume,
        provide_context=True,
    )

    log_ingestion = PythonOperator(
        task_id="log_metadata_ingestion",
        python_callable=log_metadata_ingestion,
        provide_context=True,
    )

    # Set dependencies
    create_db_schema_task >> [fetch_bus_stops, fetch_bus_routes, fetch_bus_services, fetch_passenger_volume]
    fetch_bus_stops >> transform_and_load_stops
    fetch_bus_routes >> transform_and_load_routes
    fetch_bus_services >> transform_and_load_services
    fetch_passenger_volume >> transform_and_load_passenger_volume
    [transform_and_load_stops, transform_and_load_routes, transform_and_load_services, transform_and_load_passenger_volume] >> log_ingestion
