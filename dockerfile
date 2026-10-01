FROM apache/airflow:2.4.2
USER root

# Install any system dependencies if needed
# RUN apt-get update && apt-get install -y <packages> && apt-get clean

USER airflow

# Install the Snowflake provider
RUN pip install --no-cache-dir apache-airflow-providers-snowflake==4.1.0

RUN pip install minio
RUN pip install pandas
RUN pip install requests