import datetime
import json
from monkeylearn import MonkeyLearn
from google.cloud import storage

def read_data_from_storage(bucket_name, file_name):
    """Reads a file from Cloud Storage."""
    # bucket_name = "your-bucket-name"
    # file_name = "path-to-the-file-name"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob = blob.download_as_string()
    print("Read data from {}.".format(file_name))
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
    """Extract Keywords from input text using MonkeyLearn API."""
    # input_text = "your-input-text"

    keywords = []
    ml = MonkeyLearn('f32fd99c82f87fd954d937cd179d1c665fdd57fe')
    result = ml.extractors.extract('ex_YCya9nrn', [input_text])
    for keyword in result.body[0]['extractions']: keywords.append(keyword['parsed_value'])
    return keywords


def common_keywords(transcribed_text, final_text):
    """Extract Common Keywords from ocr and transcribed text from input video"""
    # transcribed_text = "your-transcription-from-video"
    # final_text = "your-ocr-text-from-video"

    keywords = []
    math_words = [x for x in extract_keywords('\n'.join(x for x in (transcribed_text).split(" ")))]
    for x in list(extract_keywords('\n'.join(x for x in (final_text).split(" ")))): math_words.append(x)
    for word in math_words: 
        if word in str.lower(transcribed_text): keywords.append(word)
    keywords = list(set(keywords))
    print("Extracted Keywords from Input Text..")
    print("Keywords: {}".format(keywords))
    return keywords


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

    print("\nStarted Keyword Extraction..")
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    file_path = "gs://{bucket_name}/{file_name}".format(bucket_name = event['bucket'], file_name = event['name'])
    print("File Path: {}.".format(file_path))
    
    write_data = json.loads(read_data_from_storage(event['bucket'], event['name']))
    write_data['keywords'] = common_keywords(write_data['transcribed_text'], write_data['ocr_text'])
    print("Finished Keyword Extraction..")

    with open('/tmp/{file_name}'.format(file_name = event['name']), 'w') as outfile:
        json.dump(write_data, outfile)
    
    upload_blob("balance_bucket", '/tmp/{file_name}'.format(file_name = event['name']), '{file_name}_keywords.txt'.format(file_name = event['name'][:-4]))

    
