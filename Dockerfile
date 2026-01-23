FROM apache/airflow:2.8.1

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags/

ENV PYTHONPATH=${PYTHONPATH}:/opt/airflow/src