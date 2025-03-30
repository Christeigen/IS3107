"""
DAG: live_bus_data_collector
Description:
  - Collects live bus arrival info from LTA DataMall /v3/BusArrival
  - Loads bus stops from /home/houss/airflow/bus.db
  - Transforms data
  - Stores results in Redis
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

import requests
import sqlite3
from datetime import datetime, timedelta
import json
import redis  # pip install redis

##################################################
# 1) DAG CONFIG
##################################################
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),  # Must be a timedelta, not time.sleep
}

with DAG(
    dag_id="live_bus_data_collector",
    default_args=default_args,
    schedule_interval="*/7 * * * *",  # Run every 5 minutes
    catchup=False,
    tags=["bus_insights", "live_data"],
    description="Collect real-time bus arrival data and store in Redis",
) as dag:

    ##################################################
    # 2) TASK: CHECK API HEALTH
    ##################################################
    check_api_health = HttpSensor(
        task_id="check_api_health",
        http_conn_id="lta_api_connection",  # Must be set in Airflow connections
        endpoint="BusArrivalv2",           # Just checking the endpoint is reachable
        poke_interval=30,
        timeout=120,
        mode="reschedule",
    )

    ##################################################
    # 3) TASK: LOAD BUS STOP CODES FROM bus.db
    ##################################################
    def load_bus_stop_codes_func(**context):
        """Load all BusStopCode from bus.db (SQLite)."""
        db_path = "/home/houss/airflow/bus.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Assuming the BusStop table has column `BusStopCode`
        cursor.execute("SELECT BusStopCode FROM BusStop")
        rows = cursor.fetchall()

        codes = [row[0] for row in rows]
        conn.close()

        # Push codes to XCom
        return codes

    load_bus_stop_codes = PythonOperator(
        task_id="load_bus_stop_codes",
        python_callable=load_bus_stop_codes_func,
        provide_context=True,
    )

    ##################################################
    # 4) TASK: GET API KEY
    ##################################################
    def get_api_key_func(**context):
        """
        Pulls LTA API key from Airflow connection's Extra as 'AccountKey'.
        """
        conn_id = "lta_api_connection"
        connection = BaseHook.get_connection(conn_id)
        return connection.extra_dejson.get("AccountKey", None)

    get_api_key = PythonOperator(
        task_id="get_api_key",
        python_callable=get_api_key_func,
        provide_context=True,
    )

    ##################################################
    # 5) TASK: FETCH LIVE ARRIVAL DATA
    ##################################################
    def fetch_live_arrival_data_func(**context):
        """
        For each BusStopCode, call /BusArrivalv2, gather results into a single list.
        """
        # Pull codes from previous task
        codes = context["ti"].xcom_pull(task_ids="load_bus_stop_codes")
        if not codes:
            raise ValueError("No bus stop codes found.")

        # Get API key
        api_key = context["ti"].xcom_pull(task_ids="get_api_key")
        if not api_key:
            raise ValueError("No API key found in connection extras.")

        base_url = "http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2"
        headers = {"AccountKey": api_key}

        all_results = []
        for code in codes:
            params = {"BusStopCode": code}
            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                # Add the code we queried so we know it in the final data
                data["BusStopCodeQueried"] = code
                all_results.append(data)
            else:
                print(f"Error for code {code}: {response.status_code} {response.text}")

        return all_results

    fetch_live_arrival_data = PythonOperator(
        task_id="fetch_live_arrival_data",
        python_callable=fetch_live_arrival_data_func,
        provide_context=True,
    )

    ##################################################
    # 6) TASK: TRANSFORM ARRIVAL DATA
    ##################################################
    def transform_bus_arrival_data_func(**context):
        """
        Flatten each NextBus / NextBus2 / NextBus3 into separate rows.
        Return a list of records (dicts) ready for insertion in Redis.
        """
        raw_results = context["ti"].xcom_pull(task_ids="fetch_live_arrival_data")
        if not raw_results:
            print("No raw results found!")
            return []

        final_docs = []
        timestamp_now = datetime.utcnow().isoformat()

        for entry in raw_results:
            bus_stop_code = entry.get("BusStopCodeQueried")
            services = entry.get("Services", [])

            for svc in services:
                service_no = svc.get("ServiceNo")
                operator_ = svc.get("Operator")

                # We'll parse NextBus, NextBus2, NextBus3
                for idx, nextbus_key in enumerate(["NextBus", "NextBus2", "NextBus3"], start=1):
                    nb = svc.get(nextbus_key, {})
                    doc = {
                        "timestamp": timestamp_now,
                        "BusStopCode": bus_stop_code,
                        "ServiceNo": service_no,
                        "Operator": operator_,
                        "BusIndex": idx,  # 1, 2, or 3
                        "OriginCode": nb.get("OriginCode"),
                        "DestinationCode": nb.get("DestinationCode"),
                        "EstimatedArrival": nb.get("EstimatedArrival"),
                        "Monitored": nb.get("Monitored"),
                        "Latitude": nb.get("Latitude"),
                        "Longitude": nb.get("Longitude"),
                        "VisitNumber": nb.get("VisitNumber"),
                        "Load": nb.get("Load"),
                        "Feature": nb.get("Feature"),
                        "Type": nb.get("Type"),
                    }
                    final_docs.append(doc)

        return final_docs

    transform_bus_arrival_data = PythonOperator(
        task_id="transform_bus_arrival_data",
        python_callable=transform_bus_arrival_data_func,
        provide_context=True,
    )

    ##################################################
    # 7) TASK: LOAD DATA INTO REDIS
    ##################################################
    def load_data_to_redis_func(**context):
        """
        Insert documents into Redis. 
        We'll push each record onto a Redis list named 'bus_arrivals'.
        To overwrite old content, we first delete the list.
        """
        final_docs = context["ti"].xcom_pull(task_ids="transform_bus_arrival_data")
        if not final_docs:
            print("No final docs to store in Redis.")
            return

        # Retrieve Redis connection from Airflow
        redis_conn = BaseHook.get_connection("redis_default")

        # Create redis client
        r = redis.Redis(
            host=redis_conn.host,
            port=redis_conn.port,
            decode_responses=True,  # so we get str not bytes
        )

        list_name = "bus_arrivals"
        
        # Delete old content from the list
        r.delete(list_name)

        # Insert each doc into the list
        for doc in final_docs:
            r.lpush(list_name, json.dumps(doc))

        print(f"Pushed {len(final_docs)} docs into '{list_name}' list in Redis")

    load_data_to_redis = PythonOperator(
        task_id="load_data_to_redis",
        python_callable=load_data_to_redis_func,
        provide_context=True,
    )

    ##################################################
    # 8) TASK: LOG COLLECTION SUMMARY
    ##################################################
    def log_collection_summary_func(**context):
        print("Live bus data collector DAG finished successfully (using Redis)!")

    log_collection_summary = PythonOperator(
        task_id="log_collection_summary",
        python_callable=log_collection_summary_func,
        provide_context=True,
    )

    ##################################################
    # SET DAG DEPENDENCIES
    ##################################################
    get_api_key >> load_bus_stop_codes >> fetch_live_arrival_data
    fetch_live_arrival_data >> transform_bus_arrival_data
    transform_bus_arrival_data >> load_data_to_redis
    load_data_to_redis >> log_collection_summary
