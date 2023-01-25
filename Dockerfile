FROM quay.io/astronomer/astro-runtime:7.0.0
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

USER root
# Required for some ML/DS dependencies
RUN apt-get update -y
RUN apt-get install libgomp1 -y
RUN apt-get install -y git
