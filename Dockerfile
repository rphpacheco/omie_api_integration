FROM apache/airflow:2.10.4
ADD requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt