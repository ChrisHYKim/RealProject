# snowflake DB
import snowflake.connector
from airflow.models import Variable
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import json


class LoadProcess:
    def __init__(self):
        self.dataPath = Variable.get("output_path")
        self.json_raw = json.load(Variable.get("ml_json"))
        self.sprakDB = SparkSession.Builder().appName("SparkDBProcess").getOrCreate()
        df = self.sprakDB.createDataFrame(self.json_raw)
        df.write.mode("overwrite").parquet("data.parquet")

        # 건물명
        self.getBLD = Variable.get("build_names")
        self.db_init()

    # DB 초기화
    def db_init(self):
        load_dotenv()
        self.sf_user = os.getenv("sf_user")
        self.sf_pass = os.getenv("sf_pass")
        self.sf_user_url = os.getenv("sf_url")
        self.sf_db = os.getenv("sf_db")
        self.sf_schema = os.getenv("sf_schema")
        self.sf_wb = os.getenv("sf_warehouse")
        self.sf_role = os.getenv("sf_role")

    # snowflake 연결
    def conn_snowflake(self):
        return snowflake.connector.connect(
            user=self.sf_user,
            password=self.sf_pass,
            account=self.sf_user_url,
            warehouse=self.sf_wb,
            database=self.sf_db,
            schema=self.sf_schema,
        )

    def create_and_save_table(self):
        conn = self.conn_snowflake()
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE OR REPLACE DATABASE mydatabase;")
            cursor.execute(f"USE SCHEMA mydatabase.public;")
            # json 데이터 저장
            cursor.execute(f"CREATE OR REPLACE TABLE raw_real (SRC VARIANT);")
            cursor.execute(f"CREATE OR REPLACE STAGE my_stage FILE_FORMAT = (TYPE = JSON);")
            rawdata = f"data.parquet"
            if rawdata is not None:
                # 데이터 테이블로 복사
                cursor.execute(f"COPY INTO raw_real
                    FROM @my_stage/{rawdata},
                    FILE_FORMAT = (TYPE = JSON);")
                cursor.execute(f"SELECT * FROM raw_real;")
                conn.commit()
        except Exception as e:
            print("error", e)
        finally:
            cursor.close()
            conn.close()
