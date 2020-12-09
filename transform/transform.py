import json
import boto3
import botocore
import os
import zipfile
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['s3_target_bucket']
#set environ variable for testing
#S3_TARGET_BUCKET = 'fda-data-clean'

main_fields=['product_problem_flag', 'date_received', 'source_type', 'event_location', 'type_of_report', 'device', 'product_problems', 'adverse_event_flag', 'mdr_text']
device_fields=['manufacturer_d_zip_code','lot_number', 'model_number', 'generic_name', 'device_operator', 'manufacturer_d_name', 'catalog_number', 'device_name', 'medical_specialty_description', 'device_class', 'regulation_number']
openfda_device_fields=['medical_specialty_description', 'device_class', 'regulation_number']
missing_field_default = 'NA'

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print('s3 bucket: ' + bucket_name + '_ key: ' + key)
        year = key.split('/')[-2]
        file_name = key.split('/')[-1]
        zip_file_path = year + "/" + file_name
        #remove .zip for the new filename
        extracted_file_path = os.path.splitext(zip_file_path)[0]
        if not os.path.exists(year):
            os.makedirs(year)
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).download_file(key, zip_file_path)
        #extract zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(year)
        os.remove(zip_file_path)
        #open file and load json
        with open(extracted_file_path, 'r') as read_file:
            data = json.load(read_file)
        #transform data
        #extract results (remove metadata header)
        results = data['results']
        #take first 10 results for testing
        transformed_data = map(filter_fields, results)
        list_transformed_data = list(transformed_data)
        json_string = json.dumps(list_transformed_data)
        #zip transformed data
        temp_zip_file_path = '/tmp/' + zip_file_path
        with zipfile.ZipFile(temp_zip_file_path, 'w', compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.writestr(extracted_file_path, json_string)
        #write to s3
        write_file_to_s3(temp_zip_file_path, S3_TARGET_BUCKET, zip_file_path)

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

def transform_mdr(mdr):
    type_code = mdr['text_type_code']
    pythonized_string = type_code.lower().replace(' ', '_')
    return (pythonized_string, mdr['text'])

def filter_device(dic):
    if ('openfda' in dic):
        #flatten by pulling the openfda fields one level up
        if ('device_name' in dic['openfda']):
            dic.update({'device_name': dic['openfda']['device_name']})
        else:
            dic.update({'device_name': missing_field_default})
        if ('medical_specialty_description' in dic['openfda']):
            dic.update({'medical_specialty_description': dic['openfda']['medical_specialty_description']})
        else:
            dic.update({'medical_specialty_description': missing_field_default})
        if ('device_class' in dic['openfda']):
            dic.update({'device_class': dic['openfda']['device_class']})
        else:
            dic.update({'device_class': missing_field_default})
        if ('regulation_number' in dic['openfda']):
            dic.update({'regulation_number': dic['openfda']['regulation_number']})
        else:
            dic.update({'regulation_number': missing_field_default})
    else:
        dic.update({'device_name': missing_field_default})
        dic.update({'medical_specialty_description': missing_field_default})
        dic.update({'device_class': missing_field_default})
        dic.update({'regulation_number': missing_field_default})
    #then filter all unwanted fields (will remove the openfda field as well)
    return {key: dic[key] if key in device_fields else missing_field_default for key in device_fields}

def flatten_mdr(dic):
    return map(transform_mdr, dic['mdr_text'])

def filter_fields(dic):
    main_filtered = {key: dic[key] if key in dic else missing_field_default for key in main_fields}
    main_filtered.update({'device': list(map(filter_device, main_filtered['device']))})
    if ('mdr_text' in dic):
        if (dic['mdr_text']):
            main_filtered.update(list(flatten_mdr(main_filtered)))
        del main_filtered['mdr_text']
    return main_filtered