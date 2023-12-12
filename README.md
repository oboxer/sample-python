# Project Overview

## Table of Contents

- [Repository Overview](#repository-overview)
- [Setup Guide](#setup-guide)
- [Bonus README Questions](#bonus-readme-questions)

## Repository Overview

There are 2 folders in this repository: [airflow](./airflow) and [cti](./cti).

#### Airflow folder contains the following:

- A [dag](./airflow/dags/data_etl.py) named `cti_etl` that consists of two tasks, `docker_extract_transform`
  and `docker_load`.
- A [docker-compose.yml](./airflow/docker-compose.yml) file that has been taken from
  the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml).

#### CTI folder contains the following:

- An [extract_transform.py](./cti/src/ctizh/dataeng/transform/extract_transform.py) file that reads data from AWS S3,
  applies Pydantic class initialization along with data clean up, and writes it back to AWS S3.
- A Pydantic [record.py](./cti/src/ctizh/dataeng/transform/record.py) file that contains the strip and lower case
  transformation.
- A [test class](./cti/tests/cti/dataeng/transform/test_record.py) for some basic test cases.

## Setup Guide

- Prerequisites: Docker Desktop.
- Build the `cti_etl` Docker image:

```bash
cd cti
docker build -t cti_etl .
```

- Build the `airflow` environment:

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

- Access Airflow on `localhost:8080` using `airflow` as the username and password.
- [Optional] If you want to run the DAG, you'll need to set up AWS credentials, IAM role, S3 bucket, and Redshift:

```bash
# Replace values in `airflow/settings.json`
{
  # Access key and secret key for S3
  "access_key": "REPLACE_WITH_YOUR_VALUE",
  "secret_key": "REPLACE_WITH_YOUR_VALUE",
  # Parallelization settings
  "cpus": "REPLACE_WITH_YOUR_VALUE",
  "mem": "REPLACE_WITH_YOUR_VALUE",
  "shm_size": "REPLACE_WITH_YOUR_VALUE",
  # IAM is used to access AWS Redshift
  "iam": "REPLACE_WITH_YOUR_VALUE",
  "table": "REPLACE_WITH_YOUR_VALUE",
  "hostname": "REPLACE_WITH_YOUR_VALUE",
  "port": "REPLACE_WITH_YOUR_VALUE",
  "database": "REPLACE_WITH_YOUR_VALUE",
  "username": "REPLACE_WITH_YOUR_VALUE",
  "password": "REPLACE_WITH_YOUR_VALUE"
}

- With the above values replaced, navigate to http://localhost:8080/variable/list/
- Click the blue + sign, set the `Key` to `job_secret`, and use the above JSON blob as the `Val`.
```

- Turn on the DAG to run it.

## Bonus README Questions

* How did you choose to bootstrap your hosting environment?
    - For the hosting environment, I prioritized time constraints and ease of setup for the interviewer. In a production
      environment, it is best to set up Airflow with Helm Chart and run the tasks as Kubernetes pods due to their
      scalability and extensive integrations with other tools.

* What balance did you strike between hosted AWS services and self-managed components?
    - I focused on ease of use as the main factor. For the required components like Redshift, I had to use AWS services.
      S3 was not required, but it was straightforward to set up. In production, we would
      typically set up IAM roles, VPCs, and different access policies using tools like Terraform or infrastructure as
      code, rather than creating these AWS resources manually.

* How would you monitor this application? What metrics would you monitor?
    - To monitor this application, I would set up Prometheus and/or Elasticsearch to gather logs from Airflow tasks.
      Additionally, I would configure PageDuty alerts for failed tasks. If the data is mission-critical, it would be
      necessary to detect data anomalies by running statistical analysis.

* Could you extend your solution to replace a running instance of the application with little or no downtime? How?
    - The ETL code is contained within a Docker image and can run independently of Airflow. As long as the image is
      stored in an image registry like Amazon ECR, we can run it using AWS tools such as ECS or EKS, even when Airflow
      is temporarily down, ensuring minimal or no downtime.

* What would you do if you had more time?
    1. Set up AWS infrastructure as code (IaC) for all the resources, IAM roles, policies, etc.
    2. Implement Airflow using a Helm Chart for better management and scalability.
    3. Reconsider the parallelization approach for the task. Using Ray and Polars might be overkill for the given CSV
       data size. Alternatives such as using Pandas and multitasking could be more suitable. Another approach would be
       to split the raw CSV into multiple files and process it using multiple machines, like Apache Spark, though this
       file is too small for that. For this particular case, using multitasking for transform and RocksDB to dedup
       should produce good results as well.
