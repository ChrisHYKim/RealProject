import aiohttp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import xmltodict
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

# ML 라이브러리 추가
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# from pyspark.ml.evaluation import RegressionEvaluator


# snowflake DB
import snowflake.connector
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
import json


class DataProcess:
    def __init__(self, getUrl, years, buildName):
        self.getUrl = getUrl
        self.getYear = years
        # 건물명
        self.getBLD = buildName
        # shuffle partitions 개수 설정
        self.spark = (
            SparkSession.Builder()
            .appName("ReadDataProcess")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
        )
        self.stage_name = "my_csv_real"
        self.db_init()
        # bucket 생성
        self.create_stage()

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

    # Sprak Session 구성
    async def process_req(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.getUrl) as rep:
                if rep.status == 200:
                    xml_data = await rep.text()
                    return xml_data
                else:
                    print("request err")

    def process_Data(self, xml_data):
        try:
            # xml to dict convert
            dict_data = xmltodict.parse(xml_data)
            rows = dict_data.get("tbLnOpendataRtmsV", {}).get("row", [])
            # schema Type 검사
            """
            1   RCPT_YR        접수연도
            2   CGG_CD         자치구코드
            3   CGG_NM         자치구명
            4   STDG_CD        법정동코드
            5   STDG_NM        법정동명
            6   LOTNO_SE       지번구분
            7   LOTNO_SE_NM    지번구분명
            8   MNO            본번
            9   SNO            부번
            10  BLDG_NM        건물명
            11  CTRT_DAY       계약일
            12  THING_AMT      물건금액(만원)
            13  ARCH_AREA      건물면적(㎡)
            14  LAND_AREA      토지면적(㎡)
            15  FLR            층
            16  RGHT_SE        권리구분
            17  RTRCN_DAY      취소일
            18  ARCH_YR        건축년도
            19  BLDG_USG       건물용도
            20  DCLR_SE        신고구분
            21  OPBIZ_RESTAGNT_SGG_NM  신고한 개업공인중개사 시군구명
            """
            schema = StructType(
                [
                    StructField("RCPT_YR", StringType(), True),
                    # StructField("CGG_CD", StringType(), True),
                    StructField("CGG_NM", StringType(), True),
                    # StructField("STDG_CD", StringType(), True),
                    # StructField("STDG_NM", StringType(), True),
                    # StructField("LOTNO_SE", StringType(), True),
                    # StructField("LOTNO_SE_NM", StringType(), True),
                    # StructField("MNO", StringType(), True),
                    # StructField("SNO", StringType(), True),
                    StructField("BLDG_NM", StringType(), True),
                    StructField("CTRT_DAY", StringType(), True),
                    StructField("THING_AMT", StringType(), True),
                    StructField("ARCH_AREA", StringType(), True),
                    StructField("LAND_AREA", StringType(), True),
                    StructField("FLR", StringType(), True),
                    # StructField("RGHT_SE", StringType(), True),
                    # StructField("RTRCN_DAY", StringType(), True),
                    StructField("ARCH_YR", StringType(), True),
                    StructField("BLDG_USG", StringType(), True),
                    StructField("DCLR_SE", StringType(), True),
                    # StructField("OPBIZ_RESTAGNT_SGG_NM", StringType(), True)
                ]
            )
            df = self.spark.createDataFrame(rows, schema)
            # 메모리 적재
            df_cache = df.cache()

            # print(df)
            chnage_df = (
                df_cache.withColumn("RCPT_YR", F.col("RCPT_YR").cast(IntegerType()))
                .withColumn("THING_AMT", F.col("THING_AMT").cast(IntegerType()))
                .withColumn("ARCH_AREA", F.col("ARCH_AREA").cast(FloatType()))
                .withColumn("LAND_AREA", F.col("LAND_AREA").cast(FloatType()))
                .withColumn("FLR", F.col("FLR").cast(IntegerType()))
                .withColumn("CTRT_DAY", F.to_date(F.col("CTRT_DAY"), "yyyyMMdd"))
            )
            chnage_df.write.format("")
            results = chnage_df.select(
                "RCPT_YR",
                "CGG_NM",
                "BLDG_NM",
                "CTRT_DAY",
                "THING_AMT",
                "ARCH_AREA",
                "LAND_AREA",
                "FLR",
                "ARCH_YR",
                "BLDG_USG",
                "DCLR_SE",
            )
            # 결측치 제거 진행
            df_clean = results.dropna()
            # 중복되는 데이터 제거
            df_unique = df_clean.dropDuplicates(["BLDG_NM", "CTRT_DAY"])
            # df_amt_check = df_unique.filter(
            #     (F.col("THING_AMT") > 0) & (F.col("THING_AMT") < 1000000)
            # )
            df_flr = df_unique.filter(F.col("FLR") != -1)

            pd_result = df_flr.toPandas()

            self.save_to_snowflake(pd_result)
            df.unpersist()
        except Exception as proErr:
            print(proErr)

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

    # 1.snowflake 내부 스토리지 생성
    def create_stage(self):
        conn = self.conn_snowflake()
        # CURSOR 활용
        cursor = conn.cursor()
        try:
            cursor.execute(f"USE DATABASE REALEATE")
            cursor.execute(f"USE SCHEMA PUBLIC")
            cursor.execute(f"SHOW STAGES LIKE '{self.stage_name}'")
            check_stage = cursor.fetchall()
            # storage 존재 여부
            if len(check_stage) == 0:
                # csv to snowflake file upload
                cursor.execute(
                    f"""
                    CREATE OR REPLACE STAGE {self.stage_name}
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
                    """
                )
            else:
                print("stage alery exists, no created")
        except Exception as e:
            print("error", e)
        finally:
            cursor.close()
            conn.close()

    # 2.snowflake Database saves
    def save_to_snowflake(self, pd_file):
        try:
            conn = self.conn_snowflake()
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE REALEATE")
            cursor.execute(f"USE SCHEMA PUBLIC")
            current_date = pd_file["RCPT_YR"].iloc[0]
            temp_file_name = f"{current_date}.csv"
            # CSV 파일 생성
            pd_file.to_csv(temp_file_name, index=False)
            cursor.execute(f"REMOVE @{self.stage_name}/*")
            # 파일 업로드 진행
            cursor.execute(
                f"PUT file://{temp_file_name} @{self.stage_name} AUTO_COMPRESS=TRUE;"
            )
            # csv 데이터 로드 후, DB 저장 진행
            cursor.execute(
                f"""
                COPY INTO REALEATE.PUBLIC."REAL_ESTATE"
                FROM @{self.stage_name}
                FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
                ON_ERROR = 'CONTINUE';
            """
            )
            conn.commit()
            print("db success")

        except Exception as e:
            print("error", e)
        finally:
            cursor.close()
            conn.close()
            # DB 저장 후,임시 파일 삭제
            os.remove(temp_file_name)

    # snowflake 데이터를 웹페이지로 반환한다.
    def realLoadData(self):
        try:
            conn_real = self.conn_snowflake()
            cursor = conn_real.cursor()
            cursor.execute(f"USE DATABASE REALEATE")
            cursor.execute(f"USE SCHEMA PUBLIC")
            # 특정 컬럼의 데이터를 가져와야할 때 호출
            if self.getYear and self.getBLD is not None:
                query = f"""
                SELECT *
                FROM IDENTIFIER('"REALEATE"."PUBLIC"."REAL_ESTATE"')
                WHERE BLDG_USG = '{self.getBLD}' AND RCPT_YR = '{self.getYear}'
                ORDER BY CTRT_DAY DESC;
                """
                cursor.execute(query)
            # 기본값으로 데이터 가져올 경우
            else:
                query = f"""
                SELECT *
                FROM IDENTIFIER('"REALEATE"."PUBLIC"."REAL_ESTATE"')
                WHERE RCPT_YR = '{self.getYear}'
                ORDER BY CTRT_DAY DESC;
                """
                cursor.execute(query)
            result = cursor.fetchall()
            db = pd.DataFrame(
                result,
                columns=[
                    "RCPT_YR",
                    "CGG_NM",
                    "BLDG_NM",
                    "CTRT_DAY",
                    "THING_AMT",
                    "ARCH_AREA",
                    "LAND_AREA",
                    "FLR",
                    "ARCH_YR",
                    "BLDG_USG",
                    "DCLR_SE",
                ],
            )
            return db
        except Exception as reErr:
            print(reErr)
        finally:
            cursor.close()
            conn_real.close()

    # Linear 모델링 활용
    def realEateModel(self):
        try:
            # 1. 기존 데이터 로드
            df = self.realLoadData()
            pd_to_spark = self.spark.createDataFrame(df)
            model_cache = pd_to_spark.cache()

            vector_assembler = VectorAssembler(
                inputCols=["ARCH_AREA", "FLR", "ARCH_YR"], outputCol="features"
            )
            # 백터 데이터 생성
            df_data = vector_assembler.transform(pd_to_spark)
            lr = LinearRegression(
                featuresCol="features", labelCol="THING_AMT", regParam=0.01
            )

            lr_fit = lr.fit(df_data)
            predictions = lr_fit.transform(df_data)
            # 에측 결과 도출
            ml_result = predictions.select(
                "RCPT_YR", "BLDG_NM", "CTRT_DAY", "THING_AMT", "prediction", "BLDG_USG"
            )
            result_data = ml_result.collect()
            ml_json = json.dumps(
                [
                    {**row.asDict(), "CTRT_DAY": row["CTRT_DAY"].strftime("%Y-%m-%d")}
                    for row in result_data
                ]
            )
            return ml_json
        # predictions = lr_model.
        except Exception as modelErr:
            print("model err", modelErr)
        finally:
            # 모델 세션 종료unpersist()
            model_cache.unpersist()
            self.spark.stop()

            # df_data = (
            #     pd_to_spark.withColumn(
            #         "ARCH_AREA", F.col("ARCH_AREA").cast(FloatType())
            #     )
            #     .withColumn("FLR", F.col("FLR").cast(IntegerType()))
            #     .withColumn("ARCH_YR", F.col("ARCH_YR").cast(IntegerType()))
            # )
