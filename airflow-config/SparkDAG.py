from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from preData import process_data_task

default_args = {"start_date": datetime.today().strftime("%Y-%m-%d")}


# DAG 구성
with DAG(
    dag_id="SparkDAG",
    schedule_interval=timedelta(seconds=1),
    default_args=default_args,
    catchup=False,
) as dag:
    preprocess = PythonOperator(
        task_id="preprocess_data", python_callable=process_data_task
    )
    # 데이터 전처리 >> 데이터 저장
    preprocess
    # 예측 모델 분석 >> 분석 데이터 저장

    # 분석 데이터 시각화 진행
