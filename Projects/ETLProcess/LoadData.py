# snowflake DB
import snowflake.connector
from dotenv import load_dotenv
import os


class LoadProcess:
    def __init__(self, buildName):
        self.stage_name = "my_csv_real"
        # 건물명
        self.getBLD = buildName

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
