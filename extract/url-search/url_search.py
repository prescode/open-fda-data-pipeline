import json
import os
import urllib
from datetime import date, timedelta

import boto3
from botocore.client import ClientError

SITE = os.environ['site']
#test set
#SITE = 'https://download.open.fda.gov/device/event/'
START_YEAR = os.environ['start_year']
#test set
#START_YEAR = '2019'
TARGET_S3_BUCKET = os.environ['target_s3_bucket']
#test set
#TARGET_S3_BUCKET = 'fda-data-extract-urls'

def lambda_handler(event, context):
    i = 0
    #environment variables are strings
    start_year_int = int(START_YEAR)
    if (start_year_int < 1992):
        print('Error: start year must not be before 1992, entered: ' + START_YEAR)
        return
    for url in search_url(SITE, start_year_int):
        i += 1
        file_name = "url_" + str(i) + ".json"
        json_string = json.dumps({'url': url})
        write_object_to_s3(json_string, TARGET_S3_BUCKET, file_name)

def search_url(base_url, start_year, end_year = date.today().year):
    start_year = date(start_year, 6, 1)
    print("Start Year: " + str(start_year.year))
    end_year = date(end_year, 6, 1)
    print("End Year: " + str(end_year.year))
    #assumption: there will never be more than 9 files per year
    ofSearchLimit=10
    complete = False
    q = 1
    n = 1
    of = 1
    request_year = start_year
    while complete == False:
        request_url = base_url + str(request_year.year) + 'q' + str(q) + '/' + 'device-event-000' + str(n) + '-of-000' + str(of) +'.json.zip'
        try:
            print('Trying ' + request_url)
            status_code =  urllib.request.urlopen(request_url).getcode()
        except:
            #set status code
            status_code = 404
        valid_url = status_code == 200
        if(valid_url):
            for i in range(1, of + 1):
                request_url = base_url + str(request_year.year) + 'q' + str(q) + '/' + 'device-event-000' + str(i) + '-of-000' + str(of) +'.json.zip'
                #test url
                try:
                    print('Trying ' + request_url)
                    status_code = urllib.request.urlopen(request_url).getcode()
                except:
                    #set status code
                    status_code = 404
                valid_url = status_code == 200
                if(valid_url):
                    yield request_url
                else:
                    #bad bad bad
                    pass
            if(q == 4):
                if(request_year.year == end_year.year):
                    complete = True
                else:
                    #increment request year
                    request_year = request_year.replace(request_year.year + 1)
                    print("Next Request Year: " + str(request_year.year))
                    q = 1
            else:           
                #increment quarter
                q += 1
            #reset file number
            n = 1
            #reset of number
            of = 1
            continue
        else:
            if(of < ofSearchLimit):
                of += 1
            else:
                #assume we tried all filenumber of files so we have reached the end
                complete = True
                print("Stopping search after " + str(ofSearchLimit) + "iterations")

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
