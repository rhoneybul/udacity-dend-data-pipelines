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