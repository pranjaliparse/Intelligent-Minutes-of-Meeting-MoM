import datetime
import os
import json
import cv2
import io
from monkeylearn import MonkeyLearn
from google.cloud import storage as st
from google.cloud import vision
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
import urllib.request as req


creds_path = "balance-265606-c6d8d37d987d.json"
def read_data_from_storage(bucket_name, file_name):
    """Reads a file from Cloud Storage."""
    # bucket_name = "your-bucket-name"
    # file_name = "path-to-the-file-name"

    storage_client = st.Client()
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
 
    storage_client = st.Client()
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


# Extract frames
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
    if final_text:
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

    print("Started Keyword Extraction..")
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
    write_data['ocr_text'] = extract_frames(write_data['shot_changes'], write_data['video_file_path'])
    write_data['keywords'] = common_keywords(write_data['transcribed_text'], write_data['ocr_text'])
    print("Finished Keyword Extraction..")

    with open('/tmp/{file_name}'.format(file_name = event['name']), 'w') as outfile:
        json.dump(write_data, outfile)
    
    upload_blob("output_balance", '/tmp/{file_name}'.format(file_name = event['name']), '{file_name}_keywords.txt'.format(file_name = event['name'][:-4]))
