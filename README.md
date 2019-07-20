# Udacity Data Engineering Data Pipelines Project

Sparkify, a music streaming company, wants to use ETL pipelines to transform json log data from Amazon S3 into their data warehouse. 

The data consists of .csv log files, with use activity in the application, and JSON metadata which provides metadata about the songs the users listen to.

This project uses airflow to stage, populate, and check the subsequently population of songs data into Amazon Redshift. 

## Getting Started

To get started with the project, we need to start apache airflow. To do this, we can use the docker compose file which has been created in the project. To start this, we can run the following;

```
$ docker-compose up -d 
```

This will start the airflow web service, and postgres, to store the data required to run apache airflow. 

## Configuration

The following configuration is applied to the DAG to satisfy the requirements;

* The DAG does not have dependencies on past runs.
* On failure the tasks are retried 3 times.
* Retries happen every 5 minutes.
* Catchup is turned off.
* Do not email on retry.

## Operators

The following operators are created to create the data pipeline.

### Stage to Redshift Operator

The stage to redshift operator is used for copying json, or csv files into redshift.

The following parameters are provided;

* s3_file_path - the s3 path for the file to be copied into redshift.
* target_table - the redshift table for which the data will be copied into.
* file_type - whether the file to be copied into redshift is .csv, or .json.
* redshift_conn_id - the connection id for the redshift cluster to be copied into.
* s3_conn_id - the airflow connection established for s3.

The operator will copy the provided files into the redshift cluster, into the table specified, using the connection parametres provided.

### Load Dimension Operator

The load dimension operator is used to load data from the provided staging tables, into dimension tables.

The load dimension operator gives the sql statement provided to transform the data from the input table, the target table to transform into, and the redshift connection id.

### Load Fact Operator

The load fact operator is used to load data from the staging tables into fact tables. 

The load fact operator gives the sql statement provided to transform the data, the target table, and the connection id to use.

### Data Quality

The data quality operator is used to verify the quality of the data transformed into the relevant tables. 

The data quality operator gives the table of what to check. The operator takes the names for all tables to verify, and verifies whether the number of records is greater than zero.

