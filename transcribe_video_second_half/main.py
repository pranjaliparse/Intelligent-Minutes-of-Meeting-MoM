import datetime
from google.cloud import videointelligence
from google.cloud import storage as st
import json
from google.cloud.videointelligence import enums, types 
import cv2
import firebase_admin
from firebase_admin import credentials
from firebase_admin import storage
import urllib.request as req


creds_path = "balance-265606-c6d8d37d987d.json"
def read_data_from_storage(bucket_name, file_name):
    storage_client = st.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob = blob.download_as_string()
    return blob

# Generate Signed URL
def generate_signed_url(file_path):
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
    return url


# Function that takes input as milliseconds and returns the timestamp
def milliseconds(target_date_time_ms):
    base_datetime = datetime.datetime( 1970, 1, 1 )
    delta = datetime.timedelta( 0, 0, 0, target_date_time_ms )
    target_date = base_datetime + delta
    return(str(target_date).split()[1][:8])


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



# Function for transcribing videos
def transcribe_videosegments(path, segments = None):
    start = datetime.datetime.now() # Start time

    timestamp_dict = dict() # Timestamp dictionary for storing text per timestamp
    final_text = "" # Transcribed text

    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.enums.Feature.SPEECH_TRANSCRIPTION]

    config = videointelligence.types.SpeechTranscriptionConfig(
        language_code="en-US", enable_automatic_punctuation=True
    )
    video_context = videointelligence.types.VideoContext(
        segments=segments,
        speech_transcription_config=config
    )

    operation = video_client.annotate_video(
        input_uri=path, features=features, video_context=video_context
    )

    print("\nProcessing video for speech transcription.")

    result = operation.result(timeout=800)
    # There is only one annotation_result since only
    # one video is processed.
    annotation_results = result.annotation_results[0]
    print("Finished processing..")

    for speech_transcription in annotation_results.speech_transcriptions:

        # The number of alternatives for each transcription is limited by
        # SpeechTranscriptionConfig.max_alternatives.
        # Each alternative is a different possible transcription
        # and has its own confidence score.
        for alternative in speech_transcription.alternatives:
                if len(alternative.words)>0:
                    time_stamp = milliseconds(alternative.words[0].start_time.seconds * 1e3 + alternative.words[0].start_time.nanos * 1e-6)
                    #print(time_stamp,"{}".format(alternative.transcript))
                    timestamp_dict[time_stamp] = alternative.transcript
                    final_text += alternative.transcript + "\n"

    print("Time taken for transcribing: ",datetime.datetime.now()-start)
    return (final_text, timestamp_dict)

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
    video_file_path = write_data['video_file_path']
    
    cap = cv2.VideoCapture(generate_signed_url(video_file_path))
    print(cap)
    fps = cap.get(cv2.CAP_PROP_FPS)      # OpenCV2 version 2 used "CV_CAP_PROP_FPS"
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration = frame_count/fps
    
    segment = types.VideoSegment()
    segment.start_time_offset.FromSeconds(int(duration//2)-310)
    segment.end_time_offset.FromSeconds(int(duration))
    transcribed_text, timestamp_dict = transcribe_videosegments(video_file_path, segments = [segment])

    write_data['transcribed_text'] += transcribed_text
    write_data['timestamp_dict'].update(timestamp_dict)

    with open('/tmp/{file_name}'.format(file_name = event['name']), 'w') as outfile:
        json.dump(write_data, outfile)
    
    upload_blob("transcribe_video_second_half", '/tmp/{file_name}'.format(file_name = event['name']), '{file_name}'.format(file_name = event['name']))

    
