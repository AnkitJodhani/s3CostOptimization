import json
import time
import boto3
from datetime import datetime, timedelta

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
dynamodb_client = boto3.client('dynamodb')

def lambda_handler(event, context):
    bucket_name = event['queryStringParameters']['bucket_name']
    action = event['queryStringParameters']['action']

    print(bucket_name,action)

    if action == 'keep':
        s3_client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'autoDelete',
                        'Value': 'False'
                    }
                ]
            }
        )
        message = f"📌 Tagged bucket {bucket_name} with autoDelete=False"

    elif action == 'delete':

        s3_bucket = s3.Bucket(bucket_name)
        bucket_versioning = s3.BucketVersioning(bucket_name)

        if bucket_versioning.status == 'Enabled':
            s3_bucket.object_versions.delete()

        # Check if there are any objects in the bucket
        objects = list(s3_bucket.objects.all())

        if objects:
            delete_all_obj = s3_bucket.objects.all().delete()
            print(delete_all_obj)




        delete_bucket = s3_client.delete_bucket(Bucket=bucket_name)

        # s3_client.get_waiter('bucket_not_exists').wait(Bucket=bucket_name)

        print(delete_bucket)

        time.sleep(10)

        delete_response = dynamodb_client.delete_item(
            TableName='s3DateLogger',
            Key={
                "BucketName": {"S": bucket_name}
            }
        )

        print(delete_response)
        message = f"🔥 Deleted bucket: {bucket_name}"


    else:
        message = "🚫 Invalid action"

    return {
        'statusCode': 200,
        'body': json.dumps({'message': message}),
        'bucket_name': bucket_name,
        'action': action
    }