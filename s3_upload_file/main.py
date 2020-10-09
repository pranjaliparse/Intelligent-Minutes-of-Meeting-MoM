import boto
import boto.s3
import sys
from boto.s3.key import Key
from io import BytesIO
from google.cloud import storage
from google.cloud import secretmanager

def upload(event, context):    
    try:
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # get the aws access key
        awsAccessKeyId = getSecret(client, 'awsAccessKeyId')

        # get the aws access key secret
        awsSecretAccessKey = getSecret(client, 'awsSecretAccessKey')

        # aws bucket for gcp output
        awsInboundToGcp = getSecret(client, 'awsInboundToGcp')

        #bucket_name = 'balance-gcp-output-files'#awsInboundToGcp
        conn = boto.connect_s3(awsAccessKeyId, awsSecretAccessKey)
        bucket = conn.get_bucket(awsInboundToGcp)

        storage_client = storage.Client.from_service_account_json('creds.json')    
        #blobs = storage_client.list_blobs('output_balance')
        blobs = storage_client.list_blobs('output_balance', prefix=event['name'], delimiter='')

        for blob in blobs:            
            print(blob.name)
            key = boto.s3.key.Key(bucket, blob.name)
            key.set_contents_from_string(blob.download_as_string())
            #with open(blob.name) as f:
                #key.send_file(f)
        
        return 'Function completed successfully!'

    except Exception as e:
        print(e)
        return str(e)

def getSecret(client, key):
    # create the key name based on the secret key name to fetch
    name = 'projects/balance-265606/secrets/{0}/versions/latest'.format(key)
    # get the response from the secret key manager
    response = client.access_secret_version(name=name)
    # convert the response to utf-8 format and return the string value
    return str(response.payload.data.decode("UTF-8"))        

