import os
import io
import cv2
import json
import datetime
from google.cloud import videointelligence
from google.cloud import storage as st
from google.cloud import vision
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
import urllib.request as req

creds_path  = "balance-265606-c6d8d37d987d.json" # Credentials

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


# Generate Signed URL
def generate_signed_url(file_path):
    """Generate a signed URL from gcp uri"""
    # file_path = "your-gcp-file-path"
    cred = credentials.Certificate(creds_path)
    file_name = file_path.split("/")[-1]
    app = firebase_admin.initialize_app(cred, {'storageBucket': 'balance_bucket'}, name = file_name+"1")
    bucket = storage.bucket(app=app)

    def generate_image_url(blob_path):
        """ generate signed URL of a video stored on google storage. 
            Valid for 300 seconds in this case. You can increase this 
            time as per your requirement. 
        """                                                        
        blob = bucket.blob(blob_path) 
        return blob.generate_signed_url(datetime.timedelta(seconds=300), method='GET')

    url = generate_image_url(file_name)
    print("Generated a signed URL..")
    return url


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


# OCR for extracting text from frames
def detect_document(path):
    """Detects document features in an image."""
    # path = "path-of-your-document"

    client = vision.ImageAnnotatorClient()
    with io.open(path, 'rb') as image_file: content = image_file.read()
    image = vision.types.Image(content=content)
    response = client.document_text_detection(image=image)
    print("OCR Extraction done..")

    final_text = ""
    for page in response.full_text_annotation.pages:
        for block in page.blocks:
            for paragraph in block.paragraphs:
                for word in paragraph.words:
                    final_text += ''.join([
                        symbol.text for symbol in word.symbols
                    ])+" "
                final_text += "\n"

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))
    return final_text


# Detect shot changes in a video
def detect_shot_changes(path):
    """Detecting shot changes in a video"""
    # path = "your-video=path"

    start = datetime.datetime.now()
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.enums.Feature.SHOT_CHANGE_DETECTION]
    operation = video_client.annotate_video(input_uri=path, features=features)
    print("Processing video for shot change annotations.")
    result = operation.result(timeout=800)
    print("Finished processing.")

    # first result is retrieved because a single video was processed
    shot_changes = []
    for i, shot in enumerate(result.annotation_results[0].shot_annotations):
        start_time = shot.start_time_offset.seconds + shot.start_time_offset.nanos / 1e9
        end_time = shot.end_time_offset.seconds + shot.end_time_offset.nanos / 1e9
        if int(start_time+1)*1000 not in shot_changes: shot_changes.append(int(start_time+1)*1000)

    print("Time taken for detecting shot changes:", datetime.datetime.now()-start)
    if len(shot_changes)>4: shot_changes = shot_changes[5:]
    return shot_changes

  
def extract_frames(shot_changes, file_path):
    """Extracting frames from videos on extracted shot changes"""
    # shot_changes = "shot-changes-extracted-from-your-video"
    # file_path = "your-video-file-path"

    start = datetime.datetime.now()
    url = generate_signed_url(file_path)
    cap = cv2.VideoCapture(url)
    final_text = ""
    shot = 0
    success, image = cap.read()
    print("Success:", success)
    while success and shot<len(shot_changes):
        cv2.imwrite("/tmp/frame.jpg", image)     
        cap.set(cv2.CAP_PROP_POS_MSEC,(int(shot_changes[shot])+1))    # move the time
        success, image = cap.read()
        ocr_text = detect_document("/tmp/frame.jpg")
        final_text += ''.join(ocr_text)+"\n"
        shot += 1

    # release after reading    
    cap.release()
    print("Time taken for OCR", datetime.datetime.now()-start)
    return final_text
    

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

    print("\nStarted Frame Detection and OCR extraction..")
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
    video_file_path = write_data['video_file_path']

    # Detect shot changes
    shot_changes = detect_shot_changes(video_file_path)
    write_data['ocr_text'] = extract_frames(shot_changes, video_file_path)

    with open('/tmp/{file_name}'.format(file_name = event['name']), 'w') as outfile:
        json.dump(write_data, outfile)
    
    upload_blob("ocr_videos_bucket", '/tmp/{file_name}'.format(file_name = event['name']), '{file_name}'.format(file_name = event['name']))
 
