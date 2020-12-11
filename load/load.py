import json
import os
import boto3
from io import BytesIO
import zipfile
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth 
from elasticsearch.helpers import streaming_bulk

ES_INDEX_NAME = os.environ['index_name']
#ES_INDEX_NAME = 'test-index'
ES_HOST = os.environ['host']
#ES_HOST = 'vpc-fda-data-c2lwohh63nlfnhb4csbexcxt5a.us-east-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
ES_REGION = os.environ['host_region']
#ES_REGION= 'us-east-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, ES_REGION, service, session_token=credentials.token)

es = Elasticsearch(
    hosts = [{'host': ES_HOST, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

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
        #create index with mappings
        mappings = json.load(open('elasticsearch_mapping.json'))
        #try create each time (will fail if already exists but be ignored)
        es.indices.create(index=ES_INDEX_NAME, body=mappings, ignore=400)
        successes = 0
        for ok, action in streaming_bulk(
            client = es, index=ES_INDEX_NAME, actions=generate_docs(data),
        ):
            successes += ok
        print("Indexed %d documents" % (successes))

def generate_docs(data):
    for d in data:
        yield d
