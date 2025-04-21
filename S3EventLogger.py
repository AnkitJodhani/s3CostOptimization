import json
import boto3

dynamodb_client = boto3.client('dynamodb')



def lambda_handler(event, context):
    # TODO implement

    bucketName = event["detail"]["requestParameters"]["bucketName"]
    dateOfAccess = event["detail"]["eventTime"].split("T")[0]
    timeOfAccess = event["detail"]["eventTime"].split("T")[1]
    eventName = event["detail"]["eventName"]
    dateTime = event["detail"]["eventTime"]

    Item={
            "BucketName": {"S": str(bucketName)},
            "EventDateTime": {"S": str(dateTime)},
            "EventName": {"S": str(eventName)},
            "EventDate": {"S": str(dateOfAccess)},
            "EventTime": {"S": str(timeOfAccess)},
            "Status": {"S": "Active"}
        }

    print(Item)
    dynamodb_client.put_item(
        TableName='s3DateLogger',
        Item=Item
        )
    return {
        'statusCode': 200,
        'body': Item
    }
