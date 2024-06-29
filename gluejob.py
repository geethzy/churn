import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['s3_upload_to_redshift_gluejob'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['s3_upload_to_redshift_gluejob'], args)

database = 'churn_glue_database'
table_name = 'churn_glue_table'

datasource = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name=table_name
)

df = datasource.toDF()

fact_df = df.select(
    'CustomerID', 'TenureMonths', 'MonthlyCharges', 'TotalCharges', 
    'ChurnLabel', 'ChurnValue', 'ChurnScore', 'CLTV', 'ChurnReason'
)

dim_customer_df = df.select(
    'CustomerID', 'Gender', 'SeniorCitizen', 'Partner', 'Dependents'
).distinct()

dim_location_df = df.select(
    'Country', 'State', 'City', 'ZipCode', 'Latitude', 'Longitude'
).distinct()

dim_service_df = df.select(
    'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity',
    'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 
    'StreamingMovies'
).distinct()

dim_contract_df = df.select(
    'Contract', 'PaperlessBilling', 'PaymentMethod'
).distinct()

redshift_options = {
    'database': 'redshift_db',
    'user': 'churnuser',
    'password': 'churn123',
    'url': 'jdbc:redshift://rsc:5439/redshift_db',
    'aws_iam_role': 'arn:aws:iam::churnrsrole'
}

fact_df.write \
    .format("jdbc") \
    .option("url", redshift_options['url']) \
    .option("dbtable", "FactCustomerChurn") \
    .option("user", redshift_options['user']) \
    .option("password", redshift_options['password']) \
    .option("aws_iam_role", redshift_options['aws_iam_role']) \
    .mode("append") \
    .save()

dim_customer_df.write \
    .format("jdbc") \
    .option("url", redshift_options['url']) \
    .option("dbtable", "DimCustomer") \
    .option("user", redshift_options['user']) \
    .option("password", redshift_options['password']) \
    .option("aws_iam_role", redshift_options['aws_iam_role']) \
    .mode("append") \
    .save()

dim_location_df.write \
    .format("jdbc") \
    .option("url", redshift_options['url']) \
    .option("dbtable", "DimLocation") \
    .option("user", redshift_options['user']) \
    .option("password", redshift_options['password']) \
    .option("aws_iam_role", redshift_options['aws_iam_role']) \
    .mode("append") \
    .save()

dim_service_df.write \
    .format("jdbc") \
    .option("url", redshift_options['url']) \
    .option("dbtable", "DimService") \
    .option("user", redshift_options['user']) \
    .option("password", redshift_options['password']) \
    .option("aws_iam_role", redshift_options['aws_iam_role']) \
    .mode("append") \
    .save()

dim_contract_df.write \
    .format("jdbc") \
    .option("url", redshift_options['url']) \
    .option("dbtable", "DimContract") \
    .option("user", redshift_options['user']) \
    .option("password", redshift_options['password']) \
    .option("aws_iam_role", redshift_options['aws_iam_role']) \
    .mode("append") \
    .save()

job.commit()
