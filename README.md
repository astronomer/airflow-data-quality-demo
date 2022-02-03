# Data Quality Demo
This repo contains DAGs to demonstrate a variety of data quality and integrity checks.
All DAGs can be found under the dags/ folder, which is partitioned by backend data store
or provider. Specific data stores need connections and may require accounts with cloud providers. Further details are provided in the data store specific sections below.

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

#### Snowflake DAGs:
- A Snowflake account

#### Great Expectations DAGs:
- An account with service roles and tables as specified in one of the data stores above

#### SQL DAGs:
- A running SQL database

#### dbt DAGs:
- dbt profile for a backend (Redshift, BigQuery, or Snowflake)

### Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:
1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart).
2. Clone this repo locally and navigate into it.
3. Start Airflow locally by running `astro dev start`.
4. Create all necessary connections and variables - see below for specific DAG cases.
5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there.

#### Redshift DAGs:
In addition to the Getting Started steps, connections to AWS and Postgres (for Redshift) are needed to upload files to S3 and load to Redshift.
Under `Admin -> Connections` in the Airflow UI, add a new connection named `aws_default`. The `Conn Type` is `Amazon Web Services`. In the `Login` field, enter your AWS Access Key associated with your account. In the `Password` field, enter the corresponding AWS Secret Access Key. Press `Save` at the bottom.
Add another connection named `redshift_default`. The `Conn Type` is `Postgres`. The host is your Redshift host name, something like `cluster-name.XXXXXXXXXXXX.region.redshift.amazonaws.com`. The `Schema` is your Redshift schema name. `Login` is the Redshift username. `Password` is the corresponding password to access the cluster. `Port` should be 5439 (the Redshift default). Make sure your IP address is whitelisted in Redshift, and that Redshift is accepting connections outside of your VPC!

Variables needed are specified in each DAG and can be set under `Admin -> Variables` in the UI.

#### BigQuery DAGs:
In addition to the Getting Started steps, connections to GCP and BigQuery are needed to create BigQuery Datasets, tables, and insert and delete data there.
Under `Admin -> Connections` in the Airflow UI, add a new connection with Conn ID as `google_cloud_default`. The connection type is `Google Cloud`. A GCP key associated with a service account that has access to BigQuery is needed; for more information generating a key, [follow the instructions in this guide](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). The key can either be added via a path via the Keyfile Path field, or the JSON can be directly copied and pasted into the Keyfile JSON field. In the case of the Keyfile Path, a relative path is allowed, and if using Astronomer, the recommended path is under the `include/` directory, as Docker will mount all files and directories under it. Make sure the file name is included in the path. Finally, add the project ID to the Project ID field. No scopes should be needed.

Variables needed are specified in each DAG and can be set under `Admin -> Variables` in the UI.

#### Snowflake DAGs:
In addition to the Getting Started steps, a connection to Snowflake is needed to run DAGs. Under `Admin -> Connections` in the Airflow UI, add a new connection with Conn ID as `snowflake_default`. The connection type is `Snowflake`. The host field should be the full URL that you use to log into Snowflake, for example `https://[account].[region].snowflakecomputing.com`. Fill out the `Login`, `Password`, `Schema`, `Account`, `Database`, `Region`, `Role`, and `Warehouse` fields with your information.

#### Great Expectations DAGs:
In addition to the Getting Started steps, Great Expectations requires its own connections in addition to the Airflow Connections needed by other tasks in the DAG when using outside sources. These connections can be made in the file located at `include/great_expectations/uncommitted/config_variables.yml`. Note: you will have to create this file on your own, it does not come as part of the repository. Example connections in YAML are of the form:

```
my_bigquery_db:
  bigquery://[gcp-id]/[dataset]
my_snowflake_db:
  snowflake://[username]:[password]@[account].[region]/[database]/[schema]?warehouse=[warehouse]&role=[role]
my_redshift_db:
  postgresql+psycopg2://[username]:[password]@[database_uri]:5439/[default_db]
```

See the Great Expectations docs for more information on [BigQuery](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/bigquery/), [Redshift](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/redshift/), or [Snowflake](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/snowflake/). More connections can be added via the Great Expectations CLI tool

Files related to the Great Expectations DAGs can be found under `include/great_expectations/`, and the referenced SQL queries under `include/sql/great_expectations_examples/`.

Variables needed are specified in each DAG and can be set under `Admin -> Variables` in the UI.

#### SQL DAGs:
In addition to the Getting Started steps, a SQL database (sqlite, Postgres, MySQL, etc...) needs to be up and running. This database may be local or cloud-hosted. An Airflow Connection to the database is needed.

#### dbt DAGs:
In addition to the Getting Started steps, a profile for a backend needs to be created under `include/dbt/.dbt/profiles.yml`. This folder is included in `.gitignore`, so it must be created as well. The chosen backend connection must also be set up, for further instructions see the relevant section in this README.
