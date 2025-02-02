from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col
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


def process():
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
        topic = "test-topic"
        consumer = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        df = consumer.withColumn(
            "value", from_json(col("value").cast("string"), schema)
        ).select("value.*")
        logging.info(df)
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
        ).dropna()

        # 중복되는 데이터 제거
        df_unique = df_select.dropDuplicates(["BLDG_NM", "CTRT_DAY"])
        df_filtered = df_unique.filter(F.col("FLR") != -1)
        output_dir = "process/data"
        output_path = os.path.join(output_dir, "/real_data")
        df_filtered.write.format("parquet").mode("append").partitionBy(
            ["RCPT_YR", "CGG_NM"]
        ).save(output_path)
    except Exception as proErr:
        logging.error("log err", str(proErr))
    finally:
        spark.stop()
