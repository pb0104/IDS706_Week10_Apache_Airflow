# FROM apache/airflow:2.10.2

# USER airflow

# # Install Python packages
# COPY requirements.txt .
# RUN pip install --user -r requirements.txt

# USER root
# RUN apt-get update && \
#     apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# USER airflow

FROM apache/airflow:2.10.1-python3.10

# Switch to root to install system dependencies
USER root

RUN apt-get update && \
    apt-get install -y vim curl git && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Switch to airflow user
USER airflow

# Install additional Python packages inside Airflow’s venv
RUN pip install --no-cache-dir -r requirements.txt

# Clean up
WORKDIR /opt/airflow

