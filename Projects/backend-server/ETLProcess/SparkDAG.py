import asyncio
from airflow import DAG

from airflow.operators.python import PythonOperator
from Data import DataProcess
from AnalyticsML import Analytics


# data Process task
async def asyncData(**context):
    conf = context["dagRuns"].conf
    if conf:
        year = conf.get("year")
        dp = DataProcess(year)
        process_data = await dp.fetchData()
        outpath = dp.save_to_parequst(process_data)


def run_dataTask():
    asyncio.run(asyncData())


# ML Anasltics Task
def run_analyticsTask():
    analytics = Analytics()
    df = analytics.loadPreq()
    df_data = analytics.vectorProc(df)
    ml_json = analytics.trainModel(df_data)
    print(ml_json)


# snowflake upload Task
def run_uploadTask():
    pass


# DAG 구성
dag = DAG(dag_id="spark-process-dag", schedule_interval=None)
# dataTask (RealEate Row Data)
dataTask = PythonOperator(python_callable=run_dataTask, op_kwargs={"year": "2025"})
mlTask = PythonOperator(python_callable=run_analyticsTask)


# DataTask > ML Task > DataSave
dataTask >> mlTask
