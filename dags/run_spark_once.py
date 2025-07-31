
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 30),
}

with DAG(
    dag_id='run_spark_once',
    schedule_interval=None,
    default_args=default_args,
    start_date=datetime(2025, 7, 30),
    catchup=False,
    description='Run Spark job once',
    tags=['spark', 'etl'],
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id='spark_submit_etl',
        application='/opt/etl/etl_pipeline.py',
        master='spark://spark:7077', 
        conn_id='spark_default',
        verbose=True,
        spark_binary='spark-submit'
    )
