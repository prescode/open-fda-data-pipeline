import json
import os
from datetime import date, timedelta
import urllib
import boto3

START_YEAR = os.environ['start_year']
SITE = os.environ['site']

def search_url(startYear, endYear = date.today().year):
    base_url = "https://download.open.fda.gov/device/event/"
    #start year must be on or after 1992
    start_year = date(startYear, 6, 1)
    print("Start Year: " + str(start_year.year))
    end_year = date(endYear, 6, 1)
    print("End Year: " + str(end_year.year))
    ofSearchLimit=10
    complete = False
    urls = []
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
                    urls.append(request_url)
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
            #assumption: there will never be more than 9 files
            if(of < ofSearchLimit):
                of += 1
            else:
                #assume we tried all filenumber of files so we have reached the end
                complete = True
                print("Stopping search after " + str(ofSearchLimit) + "iterations")
    return urls

def lambda_handler(event, context):
    search_url(START_YEAR)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
