import json
import boto3
from datetime import datetime, timedelta
s3 = boto3.resource('s3')


bucket = 'aws-data-common'
key = '01644c2437ff161e4f3bb5e8f3190946'

# obj.get()['Body'].read().decode('utf-8') 

def lambda_handler(event, context):
    #obj = s3.Object('aws-data-common','01644c2437ff161e4f3bb5e8f3190946')
    obj = s3.Object(bucket,key)
    obj.get()['Body'].read().decode('utf-8') 
    print(obj)
