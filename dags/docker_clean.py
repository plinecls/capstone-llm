from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.models import Variable

aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

default_args = {
    "owner": "airflow",
    "description": "Clean, read and write data with DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2025, 2, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "docker_clean_dag",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:

    clean = DockerOperator(
        task_id="docker_command_clean",
        image="docker_clean:0.0.1",
        container_name="task___command_clean",
        api_version="auto",
        auto_remove='success',
        command="python3 -m capstonellm.tasks.clean --tag sql",
        environment={
            'AWS_ACCESS_KEY_ID':aws_access_key_id,
            'AWS_SECRET_ACCESS_KEY':aws_secret_access_key
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )