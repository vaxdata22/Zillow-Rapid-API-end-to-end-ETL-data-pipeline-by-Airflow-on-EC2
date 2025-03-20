
# Import necessary dependencies for the ETL pipeline DAG
from airflow import DAG
from datetime import timedelta, datetime
import json
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


# Load the Zillow API config JSON file that you saved in the airflow directory
with open('/home/ubuntu/airflow/zillow_api_config.json', 'r') as config_file:
    api_host_key = json.load(config_file)


now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")


# Define the S3 bucket that would contain the transformed data to be loaded to Redshift
s3_bucket = 'zillow-etl-data-pipeline-clean-data-bucket'


# Function to extract JSON data from API endpoint and dump it in EC2 /home/ubuntu directory
def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']
    # return headers
    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()
    
    # Specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    # Write the JSON response to a file
    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=2)  # indent for pretty formatting
    output_list = [output_file_path, file_str]
    return output_list   


# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 20),
    'email': ['donatus.enebuse@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3)
}


# Define the ETL data pipeline tasks
with DAG('zillow_etl_data_pipeline',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        # Extract the JSON data from the API endpoint
        extract_zillow_data_var = PythonOperator(
        task_id= 'tsk_extract_zillow_data_var',
        python_callable=extract_zillow_data,
        op_kwargs={'url': 'https://zillow56.p.rapidapi.com/search', 'querystring': {"location":"houston, tx"}, 'headers': api_host_key, 'date_string':dt_now_string}
        )

        # Load the JSON file into the "zillow-etl-data-pipeline-raw-data-bucket" S3 bucket landing zone
        load_to_s3 = BashOperator(
            task_id = 'tsk_load_to_s3',
            bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://zillow-etl-data-pipeline-raw-data-bucket/'
        )

        # Check if the transformed CSV file is in the "zillow-etl-data-pipeline-clean-data-bucket" S3 bucket
        is_file_in_s3_available = S3KeySensor(
        task_id='tsk_is_file_in_s3_available',
        bucket_key='{{ ti.xcom_pull("tsk_extract_zillow_data_var")[1] }}',
        bucket_name=s3_bucket,
        aws_conn_id='aws_new_conn',
        wildcard_match=False,  # Set this to True if you want to use wildcards in the prefix
        timeout=15,  # Optional: Timeout for the sensor (in seconds)
        poke_interval=3  # Optional: Time interval between S3 checks (in seconds)
        )

        # Finally load CSV data into Redshift data warehouse
        transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="tsk_transfer_s3_to_redshift",
        aws_conn_id='aws_new_conn',
        redshift_conn_id='redshift_new_conn',
        s3_bucket=s3_bucket,
        s3_key='{{ ti.xcom_pull("tsk_extract_zillow_data_var")[1] }}',
        schema="PUBLIC",
        table="zillow_data",
        copy_options=["csv IGNOREHEADER 1"]
    	)

        # Define the DAG sequence
        extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift