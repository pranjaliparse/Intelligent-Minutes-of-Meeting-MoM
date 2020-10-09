import argparse
from datetime import date, datetime
import json
from google.cloud import secretmanager
import googleapiclient.discovery

def download(request):        
    try:
        # get the secrets for the aws connection from secrets manager
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # get the aws access key
        awsAccessKeyId = getSecret(client, 'awsAccessKeyId')
        
        # get the aws access key secret
        awsSecretAccessKey = getSecret(client, 'awsSecretAccessKey')

        # get the aws source bucket form the secrets here
        source_bucket = getSecret(client, 'awsSourceBucket')

        #Create a one-time transfer from Amazon S3 to Google Cloud Storage."""
        storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')        
        description = 'Transfer job to get videos from s3. This is of type fire and forget.'
        project_id = 'balance-265606'
        start_time = datetime.utcnow()
        
        access_key_id = awsAccessKeyId
        secret_access_key = awsSecretAccessKey
        sink_bucket = 'upload_videos_bucket'

        # Edit this template with desired parameters.
        transfer_job = {
            'description': description,
            'status': 'ENABLED',
            'projectId': project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': start_time.day,
                    'month': start_time.month,
                    'year': start_time.year
                },
                'scheduleEndDate': {
                    'day': start_time.day,
                    'month': start_time.month,
                    'year': start_time.year
                }
            },
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': source_bucket,
                    'awsAccessKey': {
                        'accessKeyId': access_key_id,
                        'secretAccessKey': secret_access_key
                    }
                },
                'gcsDataSink': {
                    'bucketName': sink_bucket
                },
                'transferOptions': {
                    'deleteObjectsFromSourceAfterTransfer': 'true'
                    }
            }
        }

        result = storagetransfer.transferJobs().create(body=transfer_job).execute()
        return 'Returned transferJob: {}'.format(json.dumps(result, indent=4))

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