import datetime

from airflow import models

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators import bash_operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import date, timedelta
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage

# To define variables in Airflow UI and use it in DAG code
project_id = Variable.get('project_id')
region = Variable.get('region')

# To verify file availability using Sensor operator building below lines of the code
bucket_name = Variable.get('bucket_name')
today = datetime.datetime.today()
file_preffix = "source/dummy_"
file_suffix = ".csv"
file_date = today.strftime("%Y-%m-%d")
object_name = file_preffix + file_date + file_suffix
target_bucket_name = Variable.get('target_bucket_name')


# Python callable for branching
def check_if_bucket_exists(**kwargs):
    client = storage.Client()
    bucket_name = target_bucket_name
    try:
        client.get_bucket(bucket_name)
        return 'skip_create_bucket'  # Branch if bucket exists
    except Exception:
        return 'createNewBucket'  # Branch if bucket does NOT exist


# define default_args

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    'project_id': project_id,
    'region': region,
    'start_date': days_ago(1)
}

with models.DAG(
        "Bigquery_GCS_Airflow_Var_Sensor6",
        default_args=default_args,
        # The interval with which to schedule the DAG
        schedule_interval=datetime.timedelta(days=1)
) as dag:
    start_task = DummyOperator(
        task_id="start"
    )

    gcs_object_sensor_task = GoogleCloudStorageObjectSensor(
        task_id="File_Sensor_Check",
        bucket=bucket_name,
        object=object_name,
        timeout=120,
        poke_interval=10,
        mode="poke",
    )

    check_bucket = BranchPythonOperator(
        task_id="check_bucket_exists",
        python_callable=check_if_bucket_exists,
    )

    createNewBucket = GCSCreateBucketOperator(
        task_id="createNewBucket",
        bucket_name=target_bucket_name
    )

    copy_files = GCSToGCSOperator(
        task_id="copy_files",
        source_bucket=bucket_name,
        source_object="source/*.csv",
        destination_bucket=target_bucket_name,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,

    )

    # To create CZ dataset
    create_CZ_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_CZ_dataset",
        dataset_id="CZ",
)
# To create SZ dataset
    create_SZ_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_SZ_dataset",
        dataset_id="SZ",
)
# To create and load data into customer_lo external table
    customer_lo_external_table = BigQueryCreateExternalTableOperator(
        task_id="customer_lo_external_table",
        destination_project_dataset_table="CZ.customer_lo",
        bucket=target_bucket_name,
        source_objects=["source/customer_lo.csv"],
        skip_leading_rows=1,
        schema_fields=[
            {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cust_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "grade", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "salesman_id", "type": "INTEGER", "mode": "NULLABLE"},
    ],
)
# To create and load data into customer_ny external table
    customer_ny_external_table = BigQueryCreateExternalTableOperator(
        task_id="customer_ny_external_table",
        destination_project_dataset_table="CZ.customer_ny",
        bucket=target_bucket_name,
        source_objects=["source/customer_ny.csv"],
        skip_leading_rows=1,
        schema_fields=[
            {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cust_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "grade", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "salesman_id", "type": "INTEGER", "mode": "NULLABLE"},
    ],
)
# To create and load data into salesman_lo external table
    salesman_lo_external_table = BigQueryCreateExternalTableOperator(
        task_id="salesman_lo_external_table",
        destination_project_dataset_table="CZ.salesman_lo",
        bucket=target_bucket_name,
        source_objects=["source/salesman_lo.csv"],
        skip_leading_rows=1,
        schema_fields=[
            {"name": "salesman_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "commission", "type": "FLOAT", "mode": "NULLABLE"},
    ],
)
# To create and load data into salesman_ny external table
    salesman_ny_external_table = BigQueryCreateExternalTableOperator(
        task_id="salesman_ny_external_table",
        destination_project_dataset_table="CZ.salesman_ny",
        bucket=target_bucket_name,
        source_objects=["source/salesman_ny.csv"],
        skip_leading_rows=1,
        schema_fields=[
            {"name": "salesman_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "commission", "type": "FLOAT", "mode": "NULLABLE"},
    ],
)
# To create and load data into orders internal table
    orders_native_table_load = bash_operator.BashOperator(
        task_id='orders_native_table_load',
        bash_command=f'bq load --source_format=CSV --autodetect CZ.orders gs://{target_bucket_name}/source/orders.csv',
)
# To create results_summary internal table
    result_summary_internal_table = BigQueryCreateEmptyTableOperator(
        task_id="result_summary_internal_table",
        dataset_id="SZ",
        table_id="result_summary",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tot_purch_amt", "type": "FLOAT", "mode": "NULLABLE"},
    ],
)
# To load data into results_summary internal table
    result_summary_internal_table_load = bash_operator.BashOperator(
        task_id='result_summary_internal_table_load',
        bash_command='bq query --use_legacy_sql=false "Insert Into SZ.result_summary select sale.name,sale.city,sum(coalesce(purch_amt,0)) as tot_purch_amt from (select * from CZ.customer_lo union all select * from CZ.customer_ny)cust left outer join CZ.orders orders on cust.customer_id=orders.customer_id left outer join (select * from CZ.salesman_lo union all select * from CZ.salesman_ny)sale on sale.salesman_id=orders.salesman_id group by 1,2;"'
)

    skip_create_bucket = DummyOperator(
        task_id="skip_create_bucket"
)

    end_task = DummyOperator(
        task_id="end"
)

start_task >> gcs_object_sensor_task >> check_bucket
check_bucket >> [createNewBucket, skip_create_bucket]
createNewBucket >> copy_files
skip_create_bucket >> copy_files
copy_files >> create_CZ_dataset >> [customer_lo_external_table, customer_ny_external_table, salesman_lo_external_table,salesman_ny_external_table,orders_native_table_load] >> create_SZ_dataset >> result_summary_internal_table >> result_summary_internal_table_load >> end_task
