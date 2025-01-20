# ML 라이브러리 추가
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import IntegerType

import json


class Analytics:
    def __init__(self, pareq_path):
        self.pareq_path = pareq_path
        MAX_MEMORY = "5G"
        self.spark = (
            SparkSession.Builder()
            .appName("RealEstateModel")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.executor.memory", MAX_MEMORY)
            .config("spark.driver.memory", MAX_MEMORY)
            .getOrCreate()
        )

    def loadPreq(self):
        # 1. 기존 데이터 로드
        df = self.spark.read.parquet(self.pareq_path)
        df.show()
        return df

    # 백터 데이터 생성
    def vectorProc(self, df):
        df = df.withColumn("ARCH_YR", df["ARCH_YR"].cast(IntegerType()))
        # 결측치 제거
        df = df.filter(df["ARCH_YR"].isNotNull())
        # 각 컬럼에 대한 타입 확인
        df.printSchema()
        vector_assembler = VectorAssembler(
            inputCols=["ARCH_AREA", "FLR", "ARCH_YR"], outputCol="features"
        )
        df_data = vector_assembler.transform(df)
        return df_data

    # Linear 모델링 활용
    def trainModel(self, df_data):
        try:
            lr = LinearRegression(
                featuresCol="features", labelCol="THING_AMT", regParam=0.01
            )
            lr_fit = lr.fit(df_data)
            predictions = lr_fit.transform(df_data)
            predictions.show(5)
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
        except Exception as modelErr:
            print("model err", modelErr)
        finally:
            self.spark.stop()
