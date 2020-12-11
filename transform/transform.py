import json
import boto3
import botocore
import os
import zipfile
from io import BytesIO
from io import StringIO
from botocore.client import ClientError

S3_TARGET_BUCKET = os.environ['s3_target_bucket']
#set environ variable for testing
#S3_TARGET_BUCKET = 'fda-data-clean'
MISSING_FIELD_DEFAULT_VALUE = os.environ['missing_field_default_value']
#set environ variable for testing
#MISSING_FIELD_DEFAULT_VALUE = 'NA'

main_fields=['product_problem_flag', 'date_received', 'source_type', 'event_location', 'type_of_report', 'device', 'product_problems', 'adverse_event_flag', 'mdr_text']
device_fields=['manufacturer_d_zip_code','lot_number', 'model_number', 'generic_name', 'device_operator', 'manufacturer_d_name', 'catalog_number', 'device_name', 'medical_specialty_description', 'device_class', 'regulation_number']
openfda_device_fields=['medical_specialty_description', 'device_class', 'regulation_number']

def lambda_handler(event, context):
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print('s3 bucket: ' + bucket_name + ' key: ' + key)
        file_name = key.split('/')[-1]
        #remove .zip for the new filename
        extracted_file_name = os.path.splitext(file_name)[0]
        s3 = boto3.client('s3')
        s3_response_object = s3.get_object(Bucket=bucket_name, Key=key)
        zip_object = BytesIO(s3_response_object['Body'].read())
        #extract zip stream
        with zipfile.ZipFile(zip_object) as zip_archive:
            extracted_data = zip_archive.read(extracted_file_name)
        #extract results (remove metadata header)
        data = json.loads(extracted_data)
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

def write_object_to_s3(obj, bucket, object_name):
    # Upload the object
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body = obj, Bucket = bucket, Key = object_name)
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
            dic.update({'device_name': MISSING_FIELD_DEFAULT_VALUE})
        if ('medical_specialty_description' in dic['openfda']):
            dic.update({'medical_specialty_description': dic['openfda']['medical_specialty_description']})
        else:
            dic.update({'medical_specialty_description': MISSING_FIELD_DEFAULT_VALUE})
        if ('device_class' in dic['openfda']):
            dic.update({'device_class': dic['openfda']['device_class']})
        else:
            dic.update({'device_class': MISSING_FIELD_DEFAULT_VALUE})
        if ('regulation_number' in dic['openfda']):
            dic.update({'regulation_number': dic['openfda']['regulation_number']})
        else:
            dic.update({'regulation_number': MISSING_FIELD_DEFAULT_VALUE})
    else:
        dic.update({'device_name': MISSING_FIELD_DEFAULT_VALUE})
        dic.update({'medical_specialty_description': MISSING_FIELD_DEFAULT_VALUE})
        dic.update({'device_class': MISSING_FIELD_DEFAULT_VALUE})
        dic.update({'regulation_number': MISSING_FIELD_DEFAULT_VALUE})
    #then filter all unwanted fields (will remove the openfda field as well)
    return {key: dic[key] if key in device_fields else MISSING_FIELD_DEFAULT_VALUE for key in device_fields}

def flatten_mdr(dic):
    return map(transform_mdr, dic['mdr_text'])

def filter_fields(dic):
    main_filtered = {key: dic[key] if key in dic else MISSING_FIELD_DEFAULT_VALUE for key in main_fields}
    main_filtered.update({'device': list(map(filter_device, main_filtered['device']))})
    if ('mdr_text' in dic):
        if (dic['mdr_text']):
            main_filtered.update(list(flatten_mdr(main_filtered)))
        del main_filtered['mdr_text']
    return main_filtered