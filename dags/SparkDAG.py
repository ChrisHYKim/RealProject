import asyncio
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime, timedelta
from Data import DataProcess
from AnalyticsML import Analytics

# from LoadData import LoadProcess

default_args = {"start_date": datetime.today().strftime("%Y-%m-%d")}


# DAG 구성
with DAG(
    dag_id="SparkDAG",
    schedule=timedelta(minutes=1),
    default_args=default_args,
    catchup=False,
) as dag:

    async def processDataTask():
        try:
            year = Variable.get("years")
            urlOpen = Variable.get("open_url")
            dataProcess = DataProcess(year, urlOpen)
            xml_data = await dataProcess.fetchData()
            dataProcess.save_to_parequst(xml_data)
        except asyncio.exceptions as aio:
            print("error", aio)

    @task()
    def workData():
        try:
            asyncio.run(processDataTask())
        except Exception as err:
            print(err)

    @task()
    def workML():
        try:
            ml = Analytics()
            load_data = ml.loadPreq()
            df = ml.vectorProc(load_data)
            ml.trainModel(df)
        except Exception as mlErr:
            print("model err", mlErr)

    # @task()
    # def snowflakeProcess():
    #     try:
    #         loadProc = LoadProcess()
    #         loadProc.create_and_save_table()
    #     except Exception as err:
    #         print("process ", err)

    workData() >> workML()
