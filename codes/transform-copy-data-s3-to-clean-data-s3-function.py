
import boto3
import json
import pandas as pd

# Initialize S3 client
s3_client = boto3.client('s3')

# Define destination bucket
DESTINATION_BUCKET = "zillow-etl-data-pipeline-clean-data-bucket"

def lambda_handler(event, context):
    """AWS Lambda function to process JSON data from an S3 trigger, convert it to CSV, and store it in a different S3 bucket."""

    # Extract source bucket and object key from the event
    record = event['Records'][0]['s3']
    source_bucket = record['bucket']['name']  # Get source bucket dynamically
    object_key = record['object']['key']

    # Ensure the file is a JSON file before processing
    if not object_key.endswith('.json'):
        return {'statusCode': 400, 'body': json.dumps('Invalid file format. Expected .json')}

    # Generate target filename (convert .json to .csv, no folders in destination)
    file_name = object_key.split('/')[-1].replace('.json', '.csv')

    # Fetch and read JSON file from S3
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    data = json.loads(response['Body'].read().decode('utf-8'))

    # Convert JSON data to Pandas DataFrame
    df = pd.DataFrame(data["results"])

    # Keep only the relevant columns
    selected_columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 
                        'homeType', 'livingArea', 'price', 'rentZestimate', 'zipcode']
    
    # Filter the DataFrame for selected columns
    df = df[selected_columns]

    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)

    # Upload CSV to the destination bucket (no folder structure)
    s3_client.put_object(Bucket=DESTINATION_BUCKET, Key=file_name, Body=csv_data)

    return {'statusCode': 200, 'body': json.dumps(f"CSV saved to {DESTINATION_BUCKET}/{file_name} successfully")}
