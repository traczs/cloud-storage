import logging
import time
import boto3
from botocore.exceptions import ClientError

s3 = boto3.resource('s3')

createbucketTime = 0
populateTime = 0

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

startTime = time.time()

create_bucket("cis1300-traczs")
create_bucket("cis3110-traczs")
create_bucket("cis4010-traczs")

endTime = time.time()
createbucketTime = endTime - startTime

#from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html

for bucket in s3.buckets.all():
    print(bucket.name)

# Upload a new file
try:
    startTime = time.time()
    #cis1300 bucket
    data = open('./data/1300Assignment1.pdf', 'rb')
    s3.Bucket('cis1300-traczs').put_object(Key='1300Assignment1.pdf', Body=data)

    data = open('./data/1300Assignment2.pdf', 'rb')
    s3.Bucket('cis1300-traczs').put_object(Key='1300Assignment2.pdf', Body=data)

    data = open('./data/1300Assignment3.pdf', 'rb')
    s3.Bucket('cis1300-traczs').put_object(Key='1300Assignment3.pdf', Body=data)

    data = open('./data/1300Assignment4.pdf', 'rb')
    s3.Bucket('cis1300-traczs').put_object(Key='1300Assignment4.pdf', Body=data)

    #cis3110 bucket
    data = open('./data/3110Lecture1.pdf', 'rb')
    s3.Bucket('cis3110-traczs').put_object(Key='3110Lecture1.pdf', Body=data)

    data = open('./data/3110Lecture2.pdf', 'rb')
    s3.Bucket('cis3110-traczs').put_object(Key='3110Lecture2.pdf', Body=data)

    data = open('./data/3110Lecture3.pdf', 'rb')
    s3.Bucket('cis3110-traczs').put_object(Key='3110Lecture3.pdf', Body=data)

    data = open('./data/3110Assignment1.pdf', 'rb')
    s3.Bucket('cis3110-traczs').put_object(Key='3110Assignment1.pdf', Body=data)

    #cis4010 bucket
    data = open('./data/4010Lecture1.pdf', 'rb')
    s3.Bucket('cis4010-traczs').put_object(Key='4010Lecture1.pdf', Body=data)

    data = open('./data/4010Lecture2.pdf', 'rb')
    s3.Bucket('cis4010-traczs').put_object(Key='4010Lecture2.pdf', Body=data)

    data = open('./data/4010Assignment1.pdf', 'rb')
    s3.Bucket('cis4010-traczs').put_object(Key='4010Assignment1.pdf', Body=data)
    
    endTime = time.time()
    populateTime = endTime - startTime
    print("added data")

except:
    print("unable to upload some data")

print("Time it took to create containers: " + str(createbucketTime) + " seconds. Loading data to containers: " + str(populateTime) + " seconds")