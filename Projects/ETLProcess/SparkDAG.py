from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# DAG 구성
dag = DAG(dag_id="spark-process-dag", schedule_interval=None)
# dataTask
dataTask = SparkSubmitOperator(
    task_id="data_task",
    conn_id="spark_local",
    application="/home/chris/Documents/PythonProject/Projects/ETLProcess/Data.py",
    name="ReadDataProcess",
    dag=dag,
)

# mlTask
mlTask = SparkSubmitOperator(
    task_id="analytics",
    conn_id="spark_local",
    application="/home/chris/Documents/PythonProject/Projects/ETLProcess/AnalyticsML.py",
    name="RealEstateModel",
    dag=dag,
)

# DataTask > ML Task > DataSave
dataTask >> mlTask
