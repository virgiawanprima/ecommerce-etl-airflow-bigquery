FROM apache/airflow:3.0.6

USER airflow

# Base image bundles apache-airflow-providers-snowflake 6.4.0, which pins
# pandas<2.2. This project uses BigQuery (not Snowflake), so we remove the
# snowflake provider to unblock pandas>=2.2 per Airflow best practice of only
# shipping providers the pipeline actually uses.
RUN pip uninstall -y apache-airflow-providers-snowflake snowflake-connector-python snowflake-sqlalchemy

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.0.6" -r /requirements.txt