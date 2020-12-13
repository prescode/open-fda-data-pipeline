import json
import os
import zipfile
from collections import MutableMapping
from io import BytesIO, StringIO

import boto3
import botocore
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['s3_target_bucket']
#set environ variable for testing
#S3_TARGET_BUCKET = 'fda-data-clean'

main_fields=['product_problem_flag', 'date_received', 'source_type', 'event_location', 'type_of_report', 'device', 'product_problems', 'adverse_event_flag', 'mdr_text']
device_fields=['manufacturer_d_zip_code','lot_number', 'model_number', 'generic_name', 'device_operator', 'manufacturer_d_name', 'catalog_number']
openfda_device_fields=['device_name' 'medical_specialty_description', 'device_class', 'regulation_number']

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name, key = extract_bucket_name_and_key(record)
        print('s3 bucket: ' + bucket_name + ' s3 key: ' + key)
        file_name = key.split('/')[-1]
        #remove .zip for the new filename
        extracted_file_name = os.path.splitext(file_name)[0]
        zip_object = get_s3_object(bucket_name, key)
        #extract zip stream
        with zipfile.ZipFile(zip_object) as zip_archive:
            extracted_data = zip_archive.read(extracted_file_name)
        data = json.loads(extracted_data)
        #extract results (remove metadata header)
        results = data['results']
        #sample results for testing
        #results = results[:5]
        transformed_data = map(filter_fields, results)
        list_transformed_data = list(transformed_data)
        json_string = json.dumps(list_transformed_data)
        output_object = BytesIO()
        #zip transformed data
        with zipfile.ZipFile(output_object, 'w', compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.writestr(extracted_file_name, json_string)
            #write to s3
        output_object.seek(0)
        write_object_to_s3(output_object, S3_TARGET_BUCKET, key)

def extract_bucket_name_and_key(record):
    #extracts bucket and key name from s3 event record
    bucket_name = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    return bucket_name, key

def get_s3_object(bucket_name, key):
    #get object in memory
    s3 = boto3.client('s3')
    s3_response_object = s3.get_object(Bucket=bucket_name, Key=key)
    zip_object = BytesIO(s3_response_object['Body'].read())
    return zip_object

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

def transform_mdr(mdr):
    type_code = mdr['text_type_code']
    pythonized_string = type_code.lower().replace(' ', '_')
    return (pythonized_string, mdr['text'])

def flatten_device(dic):
    if ('openfda' in dic['device'][0]):
        #filter and
        #flatten by pulling the openfda fields one level up
        for key in dic['device'][0]['openfda']:
            if key in openfda_device_fields:
                dic['device'][0].update({key: dic['device'][0]['openfda'][key]})
        del dic['device'][0]['openfda']
    #then filter all unwanted fields (will remove the openfda field as well)
    print(dic['device'][0])
    for key in dic['device'][0]:
        if key in (device_fields + openfda_device_fields):
            dic.update({key: dic['device'][0][key]})
    del dic['device']

def flatten_mdr(dic):
    return map(transform_mdr, dic['mdr_text'])

def filter_fields(dic):
    main_filtered = {key: dic[key] if key in dic else '' for key in main_fields}
    if (len(main_filtered['device']) == 1):
        flatten_device(main_filtered)
    else:
        print('Warning: potential loss of data. Only a single device will be included')
    if ('mdr_text' in dic):
        if (dic['mdr_text']):
            main_filtered.update(list(flatten_mdr(main_filtered)))
        del main_filtered['mdr_text']
    return main_filtered