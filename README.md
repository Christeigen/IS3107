
# 🚌 Singapore Public Transport Monitoring System

A real-time dashboard & data pipeline for analyzing bus operations, built with:

- **Apache Airflow** for orchestrating data collection
- **Redis** for live bus arrivals
- **SQLite** for metadata & analytics
- **Dash** for the interactive dashboard
- **OpenAI GPT + RAG** for natural language summaries

---

## 🔧 Airflow Connection Setup

To run the DAGs successfully, you must configure the following **Airflow Connections** via the Airflow UI or CLI:

### 1. `lta_api_connection` (HTTP)

Used by both DAGs to pull real-time data from LTA DataMall.

| Field         | Value                                           |
|---------------|-------------------------------------------------|
| Conn Id       | `lta_api_connection`                            |
| Conn Type     | `HTTP`                                          |
| Host          | `https://datamall2.mytransport.sg/ltaodataservice/` |
| Extra         | `{"AccountKey": "<your-api-key-here>"}`        |

> You can register for a free AccountKey at [LTA DataMall](https://datamall.lta.gov.sg/).

---

### 2. `redis_default` (Redis)

Used by `live_bus_data_collector` and the dashboard to store and retrieve live bus arrivals.

| Field     | Value               |
|-----------|---------------------|
| Conn Id   | `redis_default`     |
| Conn Type | `Redis`             |
| Host      | `localhost` (or Docker hostname) |
| Port      | `6379`              |

---

### 3. SQLite Database (for Metadata)

- No Airflow connection needed.
- SQLite is accessed directly using the file path:
  ```
  /home/houss/airflow/bus.db
  ```

Make sure the file is created and accessible on your system (it will be created automatically by the `bus_metadata_ingestion` DAG).

---

## 📊 Running the Dashboard

1. **Install dependencies:**
   ```bash
   pip install dash dash-bootstrap-components redis openai pandas plotly pydantic pydantic-ai
   ```

2. **Run the app:**
   ```bash
   python dashboard.py
   ```

3. **Access it at:**
   ```
   http://localhost:8050
   ```

---

## 🧠 Features

- Real-time bus ETA at any stop
- Tap-in/out analytics with interactive plots
- GPT-powered chatbot to summarize bus service info
- RAG retrieval using SQLite and Redis
- LLM assistant using `pydantic-ai` + `openai`
