# Data Quality Demo
This repo contains DAGs to demonstrate a variety of data quality and integrity checks.
All DAGs can be found under the dags/ folder, which is partitioned by backend data store. Specific data stores need connections and may require accounts with cloud providers. Further details are provided in the data store specific sections below.

### Requirements
The Astronomer CLI and Docker installed locally are needed to run all DAGs in this repo. Additional requirements per project are listed below.

#### Redshift DAGs:
- An AWS account
- An S3 bucket
- An active Redshift cluster

#### BigQuery DAGs:
- A GCP account
- A service role with create, modify, and delete privileges on BigQuery
- An active GCP project with BigQuery

### Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:
1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
2. Clone this repo locally and navigate into it
3. Start Airflow locally by running `astro dev start`
4. Create all necessary connections and variables - see below for specific DAG cases
5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there

#### Redshift DAGs:

In addition to the Getting Started steps, connections to AWS and Postgres (for Redshift) are needed to upload files to S3 and load to Redshift.
Under `Admin -> Connections` in the Airflow UI, add a new connection named `aws_default`. The `Conn Type` is `Amazon Web Services`. In the `Login` field, enter your AWS Access Key associated with your account. In the `Password` field, enter the corresponding AWS Secret Access Key. Press `Save` at the bottom.
Add another connection named `redshift_default`. The `Conn Type` is `Postgres`. The host is your Redshift host name, something like `cluster-name.XXXXXXXXXXXX.region.redshift.amazonaws.com`. The `Schema` is your Redshift schema name. `Login` is the Redshift username. `Password` is the corresponding password to access the cluster. `Port` should be 5439 (the Redshift default). Make sure your IP address is whitelisted in Redshift, and that Redshift is accepting connections outside of your VPC!

Variables needed are specified in each DAG and can be set under `Admin -> Variables` in the UI.

#### BigQuery DAGs:

In addition to the Getting Started steps, connections to GCP and BigQuery are needed to create BigQuery Datasets, tables, and insert and delete data there.
Under `Admin -> Connections` in the Airflow UI, add a new connection with Conn ID as `google_cloud_default`. The connection type is `Google Cloud`. A GCP key associated with a service account that has access to BigQuery is needed; for more information generating a key, [follow the instructions in this guide](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). The key can either be added via a path via the Keyfile Path field, or the JSON can be directly copied and pasted into the Keyfile JSON field. In the case of the Keyfile Path, a relative path is allowed, and if using Astronomer, the recommended path is under the `include/` directory, as Docker will mount all files and directories under it. Make sure the file name is included in the path. Finally, add the project ID to the Project ID field. No scopes should be needed.

Variables needed are specified in each DAG and can be set under `Admin -> Variables` in the UI.
