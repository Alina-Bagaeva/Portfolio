FROM apache/airflow:2.11.0
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install apache-airflow-providers-http
RUN pip install apache-airflow-providers-telegram
