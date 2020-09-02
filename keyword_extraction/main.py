import datetime
from google.cloud import storage
import json
from monkeylearn import MonkeyLearn


def read_data_from_storage(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob = blob.download_as_string()
    return blob


def upload_blob(source_bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
 
    storage_client = storage.Client()
    bucket = storage_client.bucket(source_bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def extract_keywords(input_text):
    ml = MonkeyLearn('f32fd99c82f87fd954d937cd179d1c665fdd57fe')
    model_id = 'ex_YCya9nrn'

    keywords = []
    data = [input_text]
    result = ml.extractors.extract(model_id, data)

    for keyword in result.body[0]['extractions']:
        keywords.append(keyword['parsed_value'])

    print("Keywords:",keywords)
    return keywords

def common_keywords(transcribed_text, final_text):
    math_words = [x for x in extract_keywords('\n'.join(x for x in (transcribed_text).split(" ")))]
    for x in list(extract_keywords('\n'.join(x for x in (final_text).split(" ")))):
        math_words.append(x)

    keywords = []
    for i in math_words:
        if i in transcribed_text:
            keywords.append(i)

    keywords = list(set(keywords))
    return(keywords)

def transcribe_video_pipeline(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    file_path = "gs://{bucket_name}/{file_name}".format(bucket_name = event['bucket'], file_name = event['name'])
    print(file_path)

    write_data = json.loads(read_data_from_storage(event['bucket'], event['name']))
    write_data['keywords'] = common_keywords(write_data['transcribed_text'], write_data['ocr_text'])
    print("Finished Keyword Extraction..")

    with open('/tmp/{file_name}'.format(file_name = event['name']), 'w') as outfile:
        json.dump(write_data, outfile)
    
    upload_blob("balance_bucket", '/tmp/{file_name}'.format(file_name = event['name']), '{file_name}_keywords.txt'.format(file_name = event['name'][:-4]))

    
