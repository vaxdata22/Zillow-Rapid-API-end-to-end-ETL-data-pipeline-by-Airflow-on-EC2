
import boto3
import json

s3_client = boto3.client('s3')

# Define destination bucket
DESTINATION_BUCKET = "zillow-etl-data-pipeline-copy-data-bucket"

def lambda_handler(event, context):
    """AWS Lambda function to copy files from a dynamically triggered source bucket to a fixed destination bucket."""

    # Extract event details
    record = event['Records'][0]['s3']
    source_bucket = record['bucket']['name']  # Get source bucket dynamically
    object_key = record['object']['key']

    # Extract the filename only (removes folder structure)
    filename = object_key.split('/')[-1]

    # Copy the file to the destination bucket (no folder structure)
    s3_client.copy_object(
        Bucket=DESTINATION_BUCKET,
        Key=filename,  # No folder, just the filename
        CopySource={'Bucket': source_bucket, 'Key': object_key}
    )

    return {'statusCode': 200, 'body': json.dumps(f'File copied to {DESTINATION_BUCKET}/{filename} successfully')}

