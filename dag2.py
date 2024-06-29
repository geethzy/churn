from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLOperator
from datetime import datetime
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A data pipeline with Airflow, S3, Glue, Redshift, and Power BI',
    schedule_interval='0 12 * * *',
    catchup=False,
)

def list_s3_files(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [content['Key'] for content in response.get('Contents', [])]

list_files = PythonOperator(
    task_id='list_files',
    python_callable=list_s3_files,
    op_args=['churn', 'prefix'],
    dag=dag,
)

glue_job = AwsGlueJobOperator(
    task_id='run_glue_job',
    job_name='s3_upload_to_redshift_gluejob',
    script_location='s3://churn/s3_upload_to_redshift_gluejob.py',
    iam_role_name='churnrsrole',
    dag=dag,
)

def load_data_to_redshift():
    redshift_hook = RedshiftSQLOperator(
        task_id='load_data',
        sql="COPY churn_glue_table FROM 's3://churn/prefix' IAM_ROLE 'churnrsrole' FORMAT AS PARQUET",
        redshift_conn_id='',
        dag=dag,
    )
    redshift_hook.execute(context=None)

load_to_redshift = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_data_to_redshift,
    dag=dag,
)

list_files >> glue_job >> load_to_redshift
