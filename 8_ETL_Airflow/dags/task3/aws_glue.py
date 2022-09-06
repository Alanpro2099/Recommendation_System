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
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

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

with DAG(
    dag_id="aws_glue",
    schedule_interval='@once',
    default_args=default_args,
    start_date=datetime(2022, 7, 1),
    tags=['aws'],
    catchup=False,
) as dag:

    job_name = 'ultimate-features'
    submit_glue_job = GlueJobOperator(
        task_id='submit_glue_job',
        job_name=job_name,
        wait_for_completion=False,
        script_location=f'./dags/task3/scripts/glue_job.py',
        s3_bucket=S3_BUCKET,
        iam_role_name='AWSGlueServiceRole-imba-alan',
        create_job_kwargs={'GlueVersion': '2.0'},
    )

    wait_for_job = GlueJobSensor(
        task_id='wait_for_job',
        job_name=job_name,
        # Job ID extracted from previous Glue Job Operator task
        run_id=submit_glue_job.output,
    )

    submit_glue_job >> wait_for_job