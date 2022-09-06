from datetime import datetime, timedelta

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

S3_BUCKET = 'airflow-alan'
ATHENA_DATABASE='prd'
ATHENA_TABLE='aisles'
QUERY_READ_TABLE = f'SELECT * from {ATHENA_DATABASE}.{ATHENA_TABLE}'

with open("./dags/task2/sql/create_tables.sql") as f:
    sql_string_create_tables = f.read()

print(sql_string_create_tables)

sql_strings_tables = sql_string_create_tables.split(';')

with DAG(
    dag_id="aws_athena",
    schedule_interval='@once',
    default_args=default_args,
    start_date=datetime(2022, 7, 1),
    tags=['aws'],
    catchup=False,
) as dag:
    create_s3_bucket = S3CreateBucketOperator(
        task_id='create_s3_bucket',
        aws_conn_id='aws_default',
        bucket_name=S3_BUCKET
    )

    create_table1 = AthenaOperator(
        task_id='create_table1',
        aws_conn_id='aws_default',
        query=sql_strings_tables[0],
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )

    create_table2 = AthenaOperator(
        task_id='create_table2',
        aws_conn_id='aws_default',
        query=sql_strings_tables[1],
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )

    await_query = AthenaSensor(
        task_id='await_query',
        aws_conn_id='aws_default',
        query_execution_id=create_table1.output,
    )

    create_table3 = AthenaOperator(
        task_id='create_table3',
        aws_conn_id='aws_default',
        query=sql_strings_tables[2],
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )
    
    create_table4 = AthenaOperator(
        task_id='create_table4',
        aws_conn_id='aws_default',
        query=sql_strings_tables[3],
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )

    create_table5 = AthenaOperator(
        task_id='create_table5',
        aws_conn_id='aws_default',
        query=sql_strings_tables[4],
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/',
    )

    output_dataset = AthenaOperator(
        task_id='output_dataset',
        aws_conn_id='aws_default',
        query=sql_strings_tables[5],
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/final_features',
    )

    # read_table = AthenaOperator(
    #     task_id='read_table',
    #     aws_conn_id='AWS_default',
    #     query=QUERY_READ_TABLE,
    #     database=ATHENA_DATABASE,
    #     output_location=f's3://airflow-alan/'
    # )

    create_s3_bucket >> [create_table1,create_table2] >> await_query >> [create_table3,create_table4,create_table5] >> output_dataset