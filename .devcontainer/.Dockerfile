FROM apache/airflow:3.1.0

USER airflow

# Install Python packages
COPY requirements.txt .
RUN pip install --user -r requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y wget && rm -rf /var/lib/apt/lists/*

USER airflow
