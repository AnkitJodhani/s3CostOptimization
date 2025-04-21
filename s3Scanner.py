import json
import boto3
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError


s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
dynamodb_client = boto3.client('dynamodb')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    bucket_data = s3_client.list_buckets()

    for bucket in bucket_data["Buckets"]:
        bucket_name=bucket["Name"]
        if bucket_name == "this-bucket-is-imp-to-me" or bucket_name == "s3EventLoggingStorage":
            # Above buckets are very IMP!! Don't touch it..
            # you may want to ignore the bucket which is used by CloudTrail to store the logs or trail
            continue
        else:
            days_since_last_access = how_days_since_last_access(bucket_name)
            if days_since_last_access != None and days_since_last_access > -1:
                tag = fetch_bucket_tag(bucket_name)
                handel_bucket(bucket_name,tag,days_since_last_access)
            else:
                setup_the_date_for_old_bucket_and_store_in_db(bucket_name)


def setup_the_date_for_old_bucket_and_store_in_db(bucket_name):
    now = datetime.now()
    # Calculate the date 15 days before today
    date_15_days_ago = now - timedelta(days=15)
    # Format the date in the required format
    formatted_date = date_15_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

    bucketName = bucket_name
    dateOfAccess = formatted_date.split("T")[0]
    timeOfAccess = formatted_date.split("T")[1]
    eventName = "Manual Datetime setup"
    dateTime = formatted_date

    Item={
            "BucketName": {"S": str(bucketName)},
            "EventDateTime": {"S": str(dateTime)},
            "EventName": {"S": str(eventName)},
            "EventDate": {"S": str(dateOfAccess)},
            "EventTime": {"S": str(timeOfAccess)},
        }
    try:
        dynamodb_client.put_item(
            TableName='s3DateLogger',
            Item=Item
            )
        return Item
    except Exception as e:
        print("🚫 There is an error while setting up date for old bucket in dynamodb",e)
        return None



def handel_bucket(bucket_name,tag,days_since_last_access):
    if tag !=None and tag["Value"]=="True":
        s3_bucket = s3.Bucket(bucket_name)
        bucket_versioning = s3.BucketVersioning(bucket_name)

        if bucket_versioning.status == 'Enabled':
            s3_bucket.object_versions.delete()

        # Check if there are any objects in the bucket
        objects = list(s3_bucket.objects.all())

        if objects:
            delete_all_obj = s3_bucket.objects.all().delete()
            print(delete_all_obj)

        s3_client.delete_bucket(Bucket=bucket_name)

        time.sleep(10)

        delete_response = dynamodb_client.delete_item(
            TableName='s3DateLogger',
            Key={
                "BucketName": {"S": bucket_name}
            }
        )
        print(delete_response)
        print(f"🔥 Deleted bucket: {bucket_name}")

    elif tag !=None and tag["Value"]=="False":
        print(f"😏 Skipped bucket: {bucket_name}")
    else:
        notify_user(bucket_name,tag,days_since_last_access)


def notify_user(bucket_name,tag,days_since_last_access):
    api_gateway_url = "https://URI_OF_API_GATEWAY/prod/action"
    keep_url = f"{api_gateway_url}?bucket_name={bucket_name}&action=keep"
    delete_url = f"{api_gateway_url}?bucket_name={bucket_name}&action=delete"
    message = (f"🤯 Bucket {bucket_name} has not been accessed for {days_since_last_access} days and has no autoDelete tag. "
               f"Please choose an action: keep it or delete it.\n"
               f"🪣 Keep it: {keep_url}\n"
               f"🧼 Delete it: {delete_url}")
    try:
        res = sns_client.publish(
            TopicArn='arn:aws:sns:us-east-1:138250738702:s3BucketNotifier',
            Message=message,
            Subject='Action Required: S3 Bucket Management'
        )
        if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("📧 Email sent successfully")
        else:
            print("👎",res)
    except Exception as e:
        print("There was an error while sending email with SNS: \n",e)

def fetch_bucket_tag(bucket_name):
    try:
        data = s3_client.get_bucket_tagging(
            Bucket=bucket_name
        )
        tags = data["TagSet"]
        for tag in tags:
            if tag["Key"] == "autoDelete" and (tag["Value"]=="True" or tag["Value"]=="False"):
                return tag
            else:
                return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchTagSet':
            print(f"😥 No tags found for bucket: {bucket_name}")
            return None


def how_days_since_last_access(bucket_name):
    data = dynamodb_client.get_item(
        TableName="s3DateLogger",
        Key={"BucketName": {"S": bucket_name}}
        )
    if "Item" in data:
        last_accessed_date = data["Item"]["EventDateTime"]["S"]
        last_accessed_date = datetime.strptime(last_accessed_date, '%Y-%m-%dT%H:%M:%SZ')
        return (datetime.now() - last_accessed_date).days
    else:
        return None

