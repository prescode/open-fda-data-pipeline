import json
import boto3
import botocore
import os
from urllib.request import urlopen
from io import BytesIO
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['s3_target_bucket']
#for local testing
#S3_TARGET_BUCKET = 'fda-data-raw'

def write_object_to_s3(obj, bucket, object_name):
    # Upload the object
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body = obj, Bucket = bucket, Key = object_name)
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
        year_q = url.split('/')[-2]
        q = year_q[4:]
        year = year_q[:4]
        file_name =  url.split('/')[-1]
        new_file_path = year + "/" + q + '/' + file_name
        print(new_file_path)
        with urlopen(url) as zip_response:
            write_object_to_s3(BytesIO(zip_response.read()), S3_TARGET_BUCKET, new_file_path)