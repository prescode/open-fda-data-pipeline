import json
import boto3
import botocore
import requests
import os
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['fda-data-raw']

def write_file_to_s3(file_name, bucket, object_name = None):
    # If S3 object_name was not specified, use file_name
    print(type(file_name))
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
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print('s3 bucket: ' + bucket + '_s3 key: ' + key)
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, key)
        url = obj.get()['url'].read().decode('utf-8')
        r = requests.get(url, allow_redirects=True)
        open(key, 'w').write(r.content)
        write_file_to_s3(key, S3_TARGET_BUCKET)
            

            