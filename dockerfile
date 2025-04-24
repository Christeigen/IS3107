FROM apache/airflow:2.10.5-python3.10

USER airflow
RUN pip install supabase pandas