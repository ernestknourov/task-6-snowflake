import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv


load_dotenv()

USER = os.getenv('USER_SNOWFLAKE')
PASSWORD = os.getenv('PASSWORD')
ACCOUNT = os.getenv('ACCOUNT')
WAREHOUSE = os.getenv('WAREHOUSE')
DATABASE = os.getenv('DATABASE')
SCHEMA = os.getenv('SCHEMA')
PATH_TO_CSV = os.getenv('PATH_TO_CSV')


table_schema = """
    "_ID" VARCHAR(16777216),
    "IOS_APP_ID" BIGINT,
    "TITLE" VARCHAR(16777216),
    "DEVELOPER_NAME" VARCHAR(16777216),
    "DEVELOPER_IOS_ID" BIGINT,
    "IOS_STORE_URL" VARCHAR(16777216),
    "SELLER_OFFICIAL_WEBSITE" VARCHAR(16777216),
    "AGE_RATING" VARCHAR(16777216),
    "TOTAL_AVERAGE_RATING" FLOAT,
    "TOTAL_NUMBER_OF_RATINGS" FLOAT,
    "AVERAGE_RATING_FOR_VERSION" FLOAT,
    "NUMBER_OF_RATINGS_FOR_VERSION" INT,
    "ORIGINAL_RELEASE_DATE" VARCHAR(16777216),
    "CURRENT_VERSION_RELEASE_DATE" VARCHAR(16777216),
    "PRICE_USD" FLOAT,
    "PRIMARY_GENRE" VARCHAR(16777216),
    "ALL_GENRES" VARCHAR(16777216),
    "LANGUAGES" VARCHAR(16777216),
    "DESCRIPTION" VARCHAR(16777216)
    """

table_columns = """
    "_ID",
    "IOS_APP_ID",
    "TITLE",
    "DEVELOPER_NAME",
    "DEVELOPER_IOS_ID",
    "IOS_STORE_URL",
    "SELLER_OFFICIAL_WEBSITE",
    "AGE_RATING",
    "TOTAL_AVERAGE_RATING",
    "TOTAL_NUMBER_OF_RATINGS",
    "AVERAGE_RATING_FOR_VERSION",
    "NUMBER_OF_RATINGS_FOR_VERSION",
    "ORIGINAL_RELEASE_DATE",
    "CURRENT_VERSION_RELEASE_DATE",
    "PRICE_USD" FLOAT,
    "PRIMARY_GENRE",
    "ALL_GENRES",
    "LANGUAGES",
    "DESCRIPTION"
    """


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 11, 10, tz="UTC"),
    catchup=False,
    description='task-6-snowflake',
)
def pipeline() -> None:
    """
    This is pipeline for the 6th task. It gets data from file,
    create tables and streams, write data throughout all tables
    into master table. All actions are performed using Airflow.
    """

    def create_connection() -> snowflake.connector.connection:
        connection = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
        return connection

    @task()
    def create_table_and_streams(cursor: snowflake.connector.cursor) -> bool:
        """Create tables and streams in our storage"""
        create_raw_table = f"""CREATE OR REPLACE TABLE 
                            {DATABASE}.{SCHEMA}.RAW_TABLE ({table_schema})"""

        cursor.execute(create_raw_table)
        cursor.execute("CREATE OR REPLACE TABLE STAGE_TABLE LIKE RAW_TABLE")
        cursor.execute("CREATE OR REPLACE TABLE MASTER_TABLE LIKE STAGE_TABLE")

        cursor.execute("CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE")
        cursor.execute("CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE")
        return True

    @task()
    def insert_into_raw_table(connection: snowflake.connector.connection, path_to_csv: str) -> bool:
        """Insert raw data into snowflake table"""
        data = pd.read_csv(path_to_csv, dtype={'Developer_IOS_Id': 'Int64'}, index_col=0, skipinitialspace=True)
        data.columns = map(lambda x: str(x).upper(), data.columns)
        write_pandas(connection, data, table_name='RAW_TABLE')
        return True

    @task()
    def insert_into_stage_table(cursor: snowflake.connector.cursor) -> bool:
        """Insert data into STAGE_TABLE from raw-stream"""
        cursor.execute(f"INSERT INTO STAGE_TABLE (SELECT {table_columns} FROM RAW_STREAM)")
        return True

    @task()
    def insert_into_master_table(cursor: snowflake.connector.cursor) -> bool:
        """Insert data into MASTER_TABLE from stage-stream"""
        cursor.execute(f"INSERT INTO MASTER_TABLE (SELECT {table_columns} FROM STAGE_STREAM)")
        return True

    connection = create_connection()
    cursor = connection.cursor()

    create_table_and_streams(cursor)
    insert_into_raw_table(connection, PATH_TO_CSV)
    insert_into_stage_table(cursor)
    insert_into_master_table(cursor)


if __name__ == '__main__':
    pipeline()
