import datetime
import time

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator

project_id = Variable.get('project_id')
region = Variable.get('region')
bucket_name = Variable.get('bucket_name')
cluster_name=Variable.get('cluster_name')
source_landing_bucket = Variable.get('source_landing_bucket')
target_bucket = Variable.get('target_bucket')
today = datetime.datetime.today()
file_prefix = 'source/dummy_'
file_suffix = '.json'
file_date = today.strftime('%Y-%m-%d')
object_name = file_prefix + file_date + file_suffix

default_args={
    "project_id":project_id,
    "region":region,
    "bucket_name":bucket_name,
    "cluster_name":cluster_name,
    "start_date":days_ago(1)
}

# Python callable for branching
def check_if_bucket_exists(**kwargs):
    client = storage.Client()
    bucket_name = target_bucket
    try:
        client.get_bucket(bucket_name)
        return 'skip_create_bucket'  # Branch if bucket exists
    except Exception:
        return 'createNewBucket'  # Branch if bucket does NOT exist

def sleep_function(**kwargs):
    #sleep for 60 scenods
    time.sleep(60)
    print("Task has slept for 60 seconds.")
    

with models.DAG(
    'copy_insert_merge_job',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    tags=['dev'],
) as dag:
    
    start_task = DummyOperator(task_id='start')
    
    gcs_object_sensor_task = GoogleCloudStorageObjectSensor(
        task_id='File_Sensor_Check',
        bucket=source_landing_bucket,
        object=object_name,
        timeout=120,
        poke_interval=10,
        mode='poke',
    )
    
    check_bucket = BranchPythonOperator(
        task_id="check_bucket_exists",
        python_callable=check_if_bucket_exists,
    )

    createNewBucket = GCSCreateBucketOperator(
        task_id="createNewBucket",
        bucket_name=target_bucket,
        
    )

    skip_create_bucket = DummyOperator(
        task_id="skip_create_bucket"
    )

    copy_files = GCSToGCSOperator(
        task_id='copy_files',
        source_bucket=source_landing_bucket,
        source_object='source/*.json',
        destination_bucket=target_bucket,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    # Define sleep tasks separately
    sleep_customer_ny = PythonOperator(
    task_id='sleep_customer_ny',
    python_callable=sleep_function,
    provide_context=True,
    )

    sleep_salesman_lo = PythonOperator(
    task_id='sleep_salesman_lo',
    python_callable=sleep_function,
    provide_context=True,
    )

    sleep_salesman_ny = PythonOperator(
    task_id='sleep_salesman_ny',
    python_callable=sleep_function,
    provide_context=True,
    )

    sleep_orders = PythonOperator(
    task_id='sleep_orders',
    python_callable=sleep_function,
    provide_context=True,
    )
    
    drop = DataprocSubmitJobOperator(
        task_id='drop',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/drop_schema.sql'
            }
            },
            region=region,
    )
    
    customer_lo = DataprocSubmitJobOperator(
        task_id='customer_lo',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/customer_lo_csv.sql'
            }
            },
            region=region,
    )
    
    customer_ny = DataprocSubmitJobOperator(
        task_id='customer_ny',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/customer_ny_csv.sql'
            }
            },
            region=region,
    )
    
    salesman_lo = DataprocSubmitJobOperator(
        task_id='salesman_lo',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/salesman_lo_csv.sql'
            }
            },
            region=region,
    )
    
    salesman_ny = DataprocSubmitJobOperator(
        task_id='salesman_ny',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/salesman_ny_csv.sql'
            }
            },
            region=region,
    )
    
    orders = DataprocSubmitJobOperator(
        task_id='orders',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/orders_cvs.sql'
            }
            },
            region=region,
    )
    
    result_summary = DataprocSubmitJobOperator(
        task_id='result_summary',
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "spark_sql_job": {
                "query_file_uri": f'gs://{bucket_name}/sql/result_summary.sql'
            }
            },
            region=region,
    )
    
    end_task = DummyOperator(
        task_id='end_task',
    )
    
    start_task >> gcs_object_sensor_task >> check_bucket
    check_bucket >> createNewBucket >> copy_files
    check_bucket >> skip_create_bucket >> copy_files
    copy_files >> drop
    drop >> customer_lo
    drop >> sleep_customer_ny >> customer_ny
    drop >> sleep_salesman_lo >> salesman_lo
    drop >> sleep_salesman_ny >> salesman_ny
    drop >> sleep_orders >> orders
    [customer_lo, customer_ny, salesman_lo, salesman_ny, orders] >> result_summary >> end_task
