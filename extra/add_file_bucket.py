import logging
import boto3
from botocore.exceptions import ClientError

AWS_ACCESS_KEY_ID = 'AKIAXYKJTJDBKMZMS2IQ'  #It's in the file you saved in the previous step. Put it here in the string format.
AWS_SECRET_ACCESS_KEY = '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'  #It's in the file you saved in the previous step. Put it here in the string format.

s3_client = boto3.client('s3', region_name='us-east-2',  #choose the region you want. Here, 'us-west-2' is an example.
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

file_name = '../ETL.py'
bucket_name = 'binance-etl'
object_name = file_name #You can set your own object name or keep it the same as the original file name on the local machine.

with open(file_name, 'rb') as f:
    s3_client.upload_fileobj(f, bucket_name, object_name)