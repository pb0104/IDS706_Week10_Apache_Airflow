FROM apache/airflow:3.1.0

USER airflow

# Install Python packages
COPY requirements.txt .
RUN pip install --user -r requirements.txt

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless wget && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
