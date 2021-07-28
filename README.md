# Data Quality Demo
This repo contains DAGs to demonstrate a variety of data quality and integrity checks.

### Simple EL DAGS
The "Simple EL DAGs" are a series of three DAGs that build on each other to show how data quality and integrity checking can be done using Airflow.

### Requirements
The Astronomer CLI and Docker installed locally are needed to run all DAGs in this repo. Additional requirements per project are listed below.

#### Simple EL DAGs:
- An AWS account
- An S3 bucket
- An active Redshift cluster

### Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:
1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
2. Clone this repo locally and navigate into it
3. Start Airflow locally by running `astro dev start`
4. Create all necessary connections and variables - see below for specific DAG cases
5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there

#### Simple EL DAGs:

In addition to the setup steps above, connections to AWS and Postgres (for Redshift) are needed to upload files to S3 and load to Redshift.
Under `Admin -> Connections` in the Airflow UI, add a new connection named `aws_default`. The `Conn Type` is `Amazon Web Services`. In the `Login` field, enter your AWS Access Key associated with your account. In the `Password` field, enter the corresponding AWS Secret Access Key. Press `Save` at the bottom.
Add another connection named `redshift_default`. The `Conn Type` is `Postgres`. The host is your Redshift host name, something like `cluster-name.XXXXXXXXXXXX.region.redshift.amazonaws.com`. The `Schema` is your Redshift schema name. `Login` is the Redshift username. `Password` is the corresponding password to access the cluster. `Port` should be 5439 (the Redshift default). Make sure your IP address is whitelisted in Redshift, and that Redshift is accepting connections outside of your VPC!

Variables needed are specified in each DAG and can be set under `Admin -> Variables` in the UI.
