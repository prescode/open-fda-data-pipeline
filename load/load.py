import json
import os
import uuid
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth 
import boto3

#ES_INDEX_NAME = os.environ['index_name']
ES_INDEX_NAME = 'test-index'
#ES_HOST = os.environ['host']
ES_HOST = 'vpc-fda-data-c2lwohh63nlfnhb4csbexcxt5a.us-east-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
#ES_REGION = os.environ['host_region']
ES_REGION= 'us-east-1'

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
    #create index with mappings
    mappings = json.load(open('elasticsearch_mapping.json'))
    es.indices.create(index=ES_INDEX_NAME, body=mappings, ignore=400)
    data = json.load(open('sample_data.json'))
    singledoc = data[0]
    es.create(index=ES_INDEX_NAME, id=uuid.uuid1(), body = singledoc)





