FROM quay.io/astronomer/ap-airflow:2.1.0-2-buster-onbuild
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV GE_DATA_CONTEXT_ROOT_DIR=/usr/local/airflow/include/great_expectations
ENV DBT_PROJECT_DIR=/usr/local/airflow/include/dbt/forestfire_dq/
ENV DBT_PROFILE_DIR=/usr/local/airflow/include/dbt/.dbt/
