FROM quay.io/astronomer/astro-runtime:5.0.5
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__CORE__XCOM_BACKEND=include.gcs_xcom_backend.GCSXComBackend

USER root
# Required for some ML/DS dependencies
RUN apt-get update -y
RUN apt-get install libgomp1 -y
RUN apt-get install -y git

USER astro
ENV DBT_PROJECT_DIR_SNOWFLAKE=/usr/local/airflow/include/dbt/forestfire_dq_snowflake/
ENV DBT_PROJECT_DIR_BIGQUERY=/usr/local/airflow/include/dbt/forestfire_dq_bigquery/
ENV DBT_PROJECT_DIR_REDSHIFT=/usr/local/airflow/include/dbt/forestfire_dq_redshift/
ENV DBT_PROFILE_DIR=/usr/local/airflow/include/dbt/.dbt/
ENV PYTHONPATH=/usr/local/airflow/include:${PYTHONPATH}
