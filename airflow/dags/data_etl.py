from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator

from airflow import DAG

_SETTINGS = Variable.get("job_secret", deserialize_json=True)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
        dag_id="cti_etl",
        default_args=default_args,
        description="CTI data eng",
        catchup=False,
) as dag:
    transform_command = "extract_transform " \
                        f"--aws-access-key {_SETTINGS['access_key']} " \
                        f"--aws-secret-key {_SETTINGS['secret_key']} " \
                        f"--input-csv-path s3://ctizh/input/raw_data.csv " \
                        f"--output-parquet-dir s3://ctizh/output " \
                        f"--num-cpus {_SETTINGS['cpus']}"

    extract_transform = DockerOperator(
        task_id="docker_extract_transform",
        image="cti_etl:latest",
        api_version="auto",
        auto_remove=True,
        command=transform_command,
        cpus=float(_SETTINGS["cpus"]),
        mem_limit=_SETTINGS["mem"],
        shm_size=int(_SETTINGS["shm_size"]),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    sql_query = f"\"COPY {_SETTINGS['table']} FROM 's3://ctizh/output/valid' IAM_ROLE '{_SETTINGS['iam']}' FORMAT AS PARQUET;\""
    load_command = f"redshift_load " \
                   f"--hostname {_SETTINGS['hostname']} " \
                   f"--port {_SETTINGS['port']} " \
                   f"--database {_SETTINGS['database']} " \
                   f"--username {_SETTINGS['username']} " \
                   f"--password {_SETTINGS['password']} " \
                   f"--sql-query {sql_query}"

    load = DockerOperator(
        task_id="docker_load",
        image="cti_etl:latest",
        api_version="auto",
        auto_remove=True,
        command=load_command,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    extract_transform >> load
