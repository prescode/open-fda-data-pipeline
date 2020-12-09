import json
import boto3
import botocore
import os
import urllib.request
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['s3_target_bucket']
#for local testing
#S3_TARGET_BUCKET = 'fda-data-raw'

def write_file_to_s3(file_name, bucket, object_name = None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print('s3 bucket: ' + bucket_name + '_s3 key: ' + key)
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=key)
        body = response['Body'].read()
        url = json.loads(body)['url']
        year = url.split('/')[-2][:-2]
        file_name =  url.split('/')[-1]
        new_file_path = year + "/" + file_name
        temp_file_path = '/tmp/temp.zip'
        urllib.request.urlretrieve(url, temp_file_path)
        write_file_to_s3(temp_file_path, S3_TARGET_BUCKET, new_file_path)