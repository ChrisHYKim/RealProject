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
import asyncio
import os


class DataProcess:
    def __init__(self, year, urlOpen):
        self.year = year
        self.urlOpen = urlOpen
        # shuffle partitions 개수 설정
        self.spark = (
            SparkSession.Builder()
            .appName("ReadDataProcess")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
        )

    # Sprak Session 구성
    async def fetchData(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.urlOpen) as rep:
                if rep.status == 200:
                    xml_data = await rep.text()
                    return xml_data
                else:
                    print("request err")

    # def process_Data(self):
    #     asyncio.run()

    def save_to_parequst(self, xml_data):
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
            # 결측치 제거 진행
            df_clean = chnage_df.select(
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
            ).dropna()

            # 중복되는 데이터 제거
            df_unique = df_clean.dropDuplicates(["BLDG_NM", "CTRT_DAY"])
            df_flr = df_unique.filter(F.col("FLR") != -1)
            output_dir = "Projects/backend-server/ETLProcess/data"

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            output_path = os.path.join(
                output_dir, f"year={self.year}/realEstate_data.parquet"
            )
            df_flr.repartition(1).write.mode("overwrite").partitionBy(
                "RCPT_YR", "CGG_NM"
            ).parquet(output_path)
            # output path 반환
            return output_path

        except Exception as proErr:
            print(proErr)
        finally:
            df_flr.unpersist()
            self.spark.stop()
