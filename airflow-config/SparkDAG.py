from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from preData import process

default_args = {"start_date": datetime.today().strftime("%Y-%m-%d")}


# DAG 구성
with DAG(
    dag_id="SparkDAG",
    schedule_interval=timedelta(seconds=1),
    default_args=default_args,
    catchup=False,
) as dag:
    preprocess = PythonOperator(task_id="preprocess_data", python_callable=process)
    preprocess
