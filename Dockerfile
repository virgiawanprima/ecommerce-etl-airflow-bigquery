FROM apache/airflow:3.0.6

USER airflow

# Image dasar membundel apache-airflow-providers-snowflake 6.4.0 yang
# mengunci pandas<2.2. Proyek ini memakai BigQuery (bukan Snowflake), sehingga
# provider snowflake dihapus untuk membuka jalan bagi pandas>=2.2, sesuai
# praktik terbaik Airflow yang hanya memasukkan provider yang benar dipakai
# pipeline.
RUN pip uninstall -y apache-airflow-providers-snowflake snowflake-connector-python snowflake-sqlalchemy

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.0.6" -r /requirements.txt