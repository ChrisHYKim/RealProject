from pyspark.sql import SparkSession


# SPARK Session 생성 Function (중복 호출 방지한다.)
def create_speak_session(app_name: str):
    MAX_MEMORY = "5G"
    spark = (
        SparkSession.Builder()
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.executor.memory", MAX_MEMORY)
        .config("spark.driver.memory", MAX_MEMORY)
        .config(
            "spark.executor.extraJavaOptions",
            "-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeRefBLAS",
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeRefBLAS",
        )
        .getOrCreate()
    )
    return spark
