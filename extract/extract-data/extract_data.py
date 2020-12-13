import json
import os
from io import BytesIO
from urllib.request import urlopen

import boto3
import botocore
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['s3_target_bucket']
#for local testing
#S3_TARGET_BUCKET = 'fda-data-raw'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name, key = extract_bucket_name_and_key(record)
        print('s3 bucket: ' + bucket_name + ' s3 key: ' + key)
        obj = get_s3_object(bucket_name, key)
        #read the url key from the dictionary to get url
        url = json.loads(obj)['url']
        object_key = generate_object_key(url)
        print('S3 key for new object: ' + object_key)
        with urlopen(url) as zip_response:
            write_object_to_s3(BytesIO(zip_response.read()), S3_TARGET_BUCKET, object_key)

def generate_object_key(url):
    #generate a key(file path) with format yyyy/qq/filename
    year_q = url.split('/')[-2]
    q = year_q[4:]
    year = year_q[:4]
    file_name =  url.split('/')[-1]
    new_file_path = year + "/" + q + '/' + file_name
    return new_file_path

def get_s3_object(bucket_name, key):
    #get object in memory
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    body = response['Body'].read()
    return body
        
def extract_bucket_name_and_key(record):
    #extracts bucket and key name from s3 event record
    bucket_name = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    return bucket_name, key

def write_object_to_s3(obj, bucket, object_name):
    # Upload the object
    s3_client = boto3.client('s3')
    try:
        print('Uploading ' + object_name + ' to ' + bucket)
        s3_client.put_object(Body = obj, Bucket = bucket, Key = object_name)
    except ClientError as e:
        print(e)
        return False
    return True
