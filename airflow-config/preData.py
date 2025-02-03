from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
import logging
from SparkConf import create_speak_session
import os
import logging
import json
import aiohttp
import asyncio


# 데이터 수집 작업
async def get_real_infomation_async():
    # docker-compose 파일 내부 경로
    load_dotenv(dotenv_path="/opt/airflow/dags/.env")
    get_url = os.getenv("OPENAPI_URL")
    openkeys = os.getenv("OPENAPI_KEY")

    try:
        async with aiohttp.ClientSession() as session:
            openUrl = f"{get_url}{openkeys}/json/tbLnOpendataRtmsV/1/1000/"
            async with session.get(openUrl) as rep:
                if rep.status == 200:
                    content = await rep.json()
                    return content
    except aiohttp.ClientError as err:
        logging.error("reponse err", err)
    except json.JSONDecodeError as e:
        logging.error(f"error", {e})


# 데이터 정재 작업
def process(data):
    if data is None:
        return
    # shuffle partitions 개수 설정
    spark = create_speak_session(app_name="ReadDataProcess")

    # schema Type 검사
    schema = StructType(
        [
            StructField("RCPT_YR", StringType(), True),
            StructField("CGG_NM", StringType(), True),
            StructField("BLDG_NM", StringType(), True),
            StructField("CTRT_DAY", StringType(), True),
            StructField("THING_AMT", StringType(), True),
            StructField("ARCH_AREA", StringType(), True),
            StructField("LAND_AREA", StringType(), True),
            StructField("FLR", StringType(), True),
            StructField("ARCH_YR", StringType(), True),
            StructField("BLDG_USG", StringType(), True),
            StructField("DCLR_SE", StringType(), True),
        ]
    )

    try:
        real_estate_data = data["tbLnOpendataRtmsV"]["row"]
        rdd = spark.sparkContext.parallelize(real_estate_data)

        df = spark.createDataFrame(rdd, schema)
        df_casted = (
            df.withColumn("RCPT_YR", F.col("RCPT_YR").cast(IntegerType()))
            .withColumn("THING_AMT", F.col("THING_AMT").cast(IntegerType()))
            .withColumn("ARCH_AREA", F.col("ARCH_AREA").cast(FloatType()))
            .withColumn("LAND_AREA", F.col("LAND_AREA").cast(FloatType()))
            .withColumn("FLR", F.col("FLR").cast(IntegerType()))
            .withColumn("CTRT_DAY", F.to_date(F.col("CTRT_DAY"), "yyyyMMdd"))
        )
        # 데이터 재정의 진행
        df_select = df_casted.select(
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
        # 중복되는 데이터 제거
        df_unique = df_select.dropDuplicates(
            [
                "BLDG_NM",
                "CTRT_DAY",
            ]
        )
        # -1 인 값과 BLDG_NM 빈 값 아닌 값만 조회 (WHERE 절)
        flr_filter = df_unique.filter(F.col("FLR") != -1)
        bldg_nm_filter = flr_filter.filter(F.col("BLDG_NM").isNotNull())
        # 아파트 가격 시세 최저가, 최고가, 평군가 조회
        price_by_usage = (
            bldg_nm_filter.select(
                "RCPT_YR",
                "CGG_NM",
                "BLDG_NM",
                "CTRT_DAY",
                "FLR",
            )
            .groupBy(
                "BLDG_USG",
            )
            .agg(
                F.min("THING_AMT").alias("최저가(만원)"),
                F.max("THING_AMT").alias("최고가(만원)"),
                F.format_number(F.avg("THING_AMT"), 0).alias("평균가(만원)"),
            )
        )
        # 모든 작업이 끝나면, Snowflake에 데이터를 저장한다.
        price_by_usage.show(truncate=False)
        # 저장된 데이터 다시 내려받아 ML 예측가 분석 작업을 진행
    except Exception as proErr:
        logging.error("log err", str(proErr))
    finally:
        spark.stop()


def process_data_task():
    data = asyncio.run(get_real_infomation_async())
    if data:
        process(data)
    else:
        logging.info("None data ")
