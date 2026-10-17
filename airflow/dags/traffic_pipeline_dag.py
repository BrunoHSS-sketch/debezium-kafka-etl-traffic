
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


ETL_IMAGE = "traffic-etl:dev"

# Caminho absoluto no host onde o Docker está instalado.
DATA_HOST_PATH = (
    "/home/bruno/Projects/Python/"
    "debezium-kafka-etl-traffic/src/raw/data"
)

DATA_CONTAINER_PATH = "/app/src/raw/data"

etl_data_mount = Mount(
    source=DATA_HOST_PATH,
    target=DATA_CONTAINER_PATH,
    type="bind",
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="pipeline_transito_local",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    t1_carregar_csv = DockerOperator(
        task_id="carregar_csv_postgres",
        image=ETL_IMAGE,
        command="python /app/src/raw/data_injection.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="traffic_net",
        mounts=[etl_data_mount],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    t2_consumir_parquet = DockerOperator(
        task_id="consumir_kafka_para_parquet",
        image=ETL_IMAGE,
        command="python /app/src/consumer/kafka_polars_consumer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="traffic_net",
        mounts=[etl_data_mount],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    t1_carregar_csv >> t2_consumir_parquet
