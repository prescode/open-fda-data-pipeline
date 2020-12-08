import json
import boto3
import botocore
import os
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['fda-data-cleaned']

main_fields=['product_problem_flag', 'date_of_event', 'source_type', 'event_location', 'type_of_report', 'device', 'product_problems', 'adverse_event_flag', 'mdr_text']
device_fields=['manufacturer_d_zip_code','lot_number', 'model_number', 'generic_name', 'device_operator', 'manufacturer_d_name', 'catalog_number', 'device_name', 'medical_specialty_description', 'device_class', 'regulation_number']
openfda_device_fields=['medical_specialty_description', 'device_class', 'regulation_number']

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print('s3 bucket: ' + bucket + '_s3 key: ' + key)
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, key)
        doc = json.load(open(obj))
        transformed_data = map(filter_fields(doc), doc)
        list_transformed_data = list(transformed_data)
        #re-use the key from the read file for the destination file
        with open(key, "w") as write_file:
            json.dump(list_transformed_data, write_file, separators=(',', ':'))
            write_file_to_s3(write_file.name, S3_TARGET_BUCKET)

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
    
def transform_mdr(mdr):
    type_code = mdr['text_type_code']
    pythonized_string = type_code.lower().replace(' ', '_')
    return (pythonized_string, mdr['text'])

def filter_device(dic):
    #flatten by pulling the openfda fields one level up
    #print(json.dumps(dict(dic), indent=1, sort_keys=True, separators=(',',': ')))
    dic.update({'device_name': dic['openfda']['device_name']})
    dic.update({'medical_specialty_description': dic['openfda']['medical_specialty_description']})
    dic.update({'device_class': dic['openfda']['device_class']})
    dic.update({'regulation_number': dic['openfda']['regulation_number']})
    #then filter all unwanted fields (will remove the openfda field as well)
    return {key: dic[key] for key in device_fields}

def flatten_mdr(dic):
    #print(json.dumps(dict(dic), indent=1, sort_keys=True, separators=(',',': ')))
    if ('mdr_text' in dic):
        return map(transform_mdr, dic['mdr_text'])

def filter_fields(dic):
    main_filtered = {key: dic[key] for key in main_fields}
    main_filtered.update({'device': list(map(filter_device, main_filtered['device']))})
    main_filtered.update(list(flatten_mdr(main_filtered)))
    del main_filtered['mdr_text']
    return main_filtered