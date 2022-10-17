import json
import tweepy
import datetime
import boto3
import uuid
import base64
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "twitter-bearer-token"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret
            
    # Your code goes here. 
    
    
bearer_token = get_secret()


def lambda_handler(event, context):
    # TODO implement
    
    # Creating twitter client
    client = tweepy.Client(json.loads(bearer_token)['twitter-bearer'])
    
    # Creating Kinesis client
    kinesis_client = boto3.client('kinesis')
    
    # Declaring endtime and starttime for tweets
    date_and_time = datetime.datetime.utcnow()
    time_change = datetime.timedelta(minutes=-5)
    time_change1 = datetime.timedelta(seconds=-10)
    new_time = date_and_time + time_change
    date_and_time = date_and_time + time_change1
    
    # Search Recent Tweets
    # This endpoint/method returns Tweets from last 5 minutes
    response = client.search_recent_tweets("#HOTD",max_results = 100,end_time = date_and_time,start_time = new_time,)
    
    # In this case, the data field of the Response returned is a list of Tweet
    # objects
    tweets = response.data
    
    # Each Tweet object has default id and text fields
    for tweet in tweets:
        print(json.dumps(tweet.data))
        
        output = kinesis_client.put_record(StreamName='twitter-feed-data-stream',
                                Data=bytes(json.dumps(tweet.data),'utf-8'),
                                PartitionKey=str(uuid.uuid4()))
    
    return {
        'statusCode': 200,
        'body': output
    }
