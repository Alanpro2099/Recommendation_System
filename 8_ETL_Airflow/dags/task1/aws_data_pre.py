from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import logging
import boto3
import os
import glob

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.models import Variable

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

S3_BUCKET="imba-alan2"
BUCKET_REGION='ap-southeast-2'
KEY_ID=Variable.get('aws_key_id')
ACCESS_KEY=Variable.get('aws_secret_access_key')
FILENAMES1 = glob.glob("./dags/task1/data_files/*.csv")
print(FILENAMES1)
print("There are ", len(FILENAMES1), " files in the first file set")
FILENAMES2 = glob.glob("./dags/task1/data_files/*.gz")
print("There are ", len(FILENAMES2), " files in the second file set")

GLUE_CRAWLER_CONFIG = {
    'Name': 'imba-alan2-crawler',
    'Role': 'arn:aws:iam::498938378154:role/service-role/AWSGlueServiceRole-imba-alan',
    'DatabaseName': 'prd2',
    'Targets': {
        'S3Targets': [
            {
                'Path': f'{S3_BUCKET}/data',
            }
        ]
    },
}

def upload_file():
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # Upload the file
    s3_client = boto3.client(service_name='s3', 
                            region_name=BUCKET_REGION, 
                            aws_access_key_id=KEY_ID, 
                            aws_secret_access_key=ACCESS_KEY)
    for file_name in FILENAMES1:
        file_NM = os.path.basename(file_name)
        object_name = 'data/'+ file_NM.split(".")[0] + '/' + file_NM
        response = s3_client.upload_file(file_name,S3_BUCKET, object_name)
        print(file_NM, "has been uploaded")

    for file_name in FILENAMES2:
        file_NM = os.path.basename(file_name)
        object_name = 'data/order_products/' + file_NM
        response = s3_client.upload_file(file_name, S3_BUCKET, object_name)
        print(file_NM, "has been uploaded")

with DAG(
    dag_id = "aws_data_pre",
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

    upload_files = PythonOperator(
        task_id = 'upload_files',
        python_callable = upload_file,
        dag=dag
    )

    crawl_s3 = GlueCrawlerOperator(
        task_id='crawl_s3',
        aws_conn_id='aws_default',
        config=GLUE_CRAWLER_CONFIG,
        wait_for_completion=False,
    )

    wait_for_crawl = GlueCrawlerSensor(
        task_id='wait_for_crawl', 
        aws_conn_id='aws_default',
        crawler_name='imba-alan2-crawler'
    )

    create_s3_bucket >> upload_files >> crawl_s3 >> wait_for_crawl

