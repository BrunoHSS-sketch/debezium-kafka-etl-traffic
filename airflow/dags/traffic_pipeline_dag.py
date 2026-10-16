from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
        "pipeline_transito_local",
        default_args=default_args,
        schedule="@daily",
        catchup=False,
) as dag:
    t1_carregar_csv = BashOperator(
        task_id="carregar_csv_postgres",
        bash_command="python /opt/airflow/src/raw/data_injection.py",
    )

    t2_consumir_parquet = BashOperator(
        task_id="consumir_kafka_para_parquet",
        bash_command="python /opt/airflow/src/consumer/kafka_polars_consumer.py",
    )

    t1_carregar_csv >> t2_consumir_parquet