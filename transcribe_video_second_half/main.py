import json
import cv2
import csv
import datetime
import firebase_admin
import urllib.request as req
from google.cloud import videointelligence
from google.cloud import storage as st
from firebase_admin import credentials
from firebase_admin import storage
from webvtt import WebVTT, Caption
from google.cloud.videointelligence import enums, types


creds_path  = "balance-265606-c6d8d37d987d.json" 
# Credentials

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



# Function that takes input as milliseconds and returns the timestamp
def milliseconds(target_date_time_ms):
  base_datetime = datetime.datetime( 1970, 1, 1 )
  delta = datetime.timedelta( 0, 0, 0, target_date_time_ms )
  target_date = base_datetime + delta
  return(str(target_date).split()[1][:12])



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



def transcribe_videosegments(path, segments = None):
    """
    Video-to-Text for segments of videos.
    Example:
    segment = types.VideoSegment()
    segment.start_time_offset.FromSeconds(0)
    segment.end_time_offset.FromSeconds(int(duration//2)-300)
    """
    # path = "your-file-path"
    # segments = "video-segments"

    start = datetime.datetime.now() # Start time
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.enums.Feature.SPEECH_TRANSCRIPTION]
    config = videointelligence.types.SpeechTranscriptionConfig(
        language_code="en-US", enable_automatic_punctuation=True,
        enable_speaker_diarization = True
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
    speech_transcription_with_speakertag = annotation_results.speech_transcriptions[-1]
    print("Finished processing..")
    print("Time taken for transcribing: ",datetime.datetime.now()-start)

    return (result, speech_transcription_with_speakertag)



def subtitle_generation(response, vtt, bin_size=3):
    """We define a bin of time period to display the words in sync with audio. 
    Here, bin_size = 3 means each bin is of 3 secs. 
    All the words in the interval of 3 secs in result will be grouped togather."""
    # response = "videoIntelligence-response-to-API"

    transcribed_text = ""
    index = 0
    flag = None

    for speech_transcription in response.annotation_results[0].speech_transcriptions:
      # The number of alternatives for each transcription is limited by
      # SpeechTranscriptionConfig.max_alternatives.
      # Each alternative is a different possible transcription
      # and has its own confidence score.
      for alternative in speech_transcription.alternatives:
        try:
            if alternative.words[0].start_time.seconds:
                # bin start -> for first word of result
                start_sec = alternative.words[0].start_time.seconds 
                start_microsec = alternative.words[0].start_time.nanos * 0.001
            else:
                # bin start -> For First word of response
                start_sec = 0
                start_microsec = 0 
            end_sec = start_sec + bin_size # bin end sec
            
            # for last word of result
            last_word_end_sec = alternative.words[-1].end_time.seconds
            last_word_end_microsec = alternative.words[-1].end_time.nanos * 0.001
            
            # bin transcript
            transcript = alternative.words[0].word
            
            index += 1 # subtitle index

            for i in range(len(alternative.words) - 1):
                try:
                    word = alternative.words[i + 1].word
                    word_start_sec = alternative.words[i + 1].start_time.seconds
                    word_start_microsec = alternative.words[i + 1].start_time.nanos * 0.001 # 0.001 to convert nana -> micro
                    word_end_sec = alternative.words[i + 1].end_time.seconds
                    word_end_microsec = alternative.words[i + 1].end_time.nanos * 0.001

                    if word_end_sec < end_sec and not('!' in alternative.words[i].word or '?' in alternative.words[i].word or '.' in alternative.words[i].word):
                        transcript = transcript + " " + word
                    else:
                        previous_word_end_sec = alternative.words[i].end_time.seconds
                        previous_word_end_microsec = alternative.words[i].end_time.nanos * 0.001
                        
                        # append bin transcript
                        start = str(datetime.timedelta(0, start_sec, start_microsec))[:12]
                        end = str(datetime.timedelta(0, previous_word_end_sec, previous_word_end_microsec))[:12]
                        if len(start)<=8: start += ".000"
                        if len(end)<=8: end += ".000"
                        if flag and flag == start: break
                        if not(flag): flag = start
                        caption = Caption(start, end, transcript)
                        transcribed_text += transcript + " "
                        vtt.captions.append(caption)
                        
                        # reset bin parameters
                        start_sec = word_start_sec
                        start_microsec = word_start_microsec
                        end_sec = start_sec + bin_size
                        transcript = alternative.words[i + 1].word
                        index += 1
                except IndexError:
                    pass
            # append transcript of last transcript in bin
            start = str(datetime.timedelta(0, start_sec, start_microsec))[:12]
            end = str(datetime.timedelta(0, last_word_end_sec, last_word_end_microsec))[:12]
            if len(start)<=8: start += ".000"
            if len(end)<=8: end += ".000"
            if flag and flag == start: break
            if not(flag): flag = start
            caption = Caption(start, end, transcript)
            vtt.captions.append(caption)
            index += 1
        except IndexError:
            pass
    
    # turn transcription list into subtitles
    return (transcribed_text, vtt)

    
 
def upload_csv(speech_transcription_with_speakertag, file_name):
    speakertag = dict()
    prev = None

    with open('/tmp/{file_name}'.format(file_name = file_name), 'a+') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t')
        for word in speech_transcription_with_speakertag.alternatives[0].words:
            if word.speaker_tag not in speakertag:
                speakertag[word.speaker_tag] = [-1, ""]
            start = milliseconds(word.start_time.seconds * 1e3 + word.start_time.nanos * 1e-6)
            if len(start)<=8: start += ".000"
            speakertag[word.speaker_tag][0] = start
            speakertag[word.speaker_tag][1] += word.word + " "

            if prev and speakertag[prev][1] and prev!=word.speaker_tag:
                tsv_writer.writerow([speakertag[prev][0], "Speaker {tag}".format(tag = prev), speakertag[prev][1]])
                # print("{time}: Speaker {tag}: {text}\n".format(time = speakertag[prev][0], tag = prev, text = speakertag[prev][1]))
                speakertag[prev] = [-1, ""]  

            prev = word.speaker_tag
    
    upload_blob("output_balance", '/tmp/{file_name}'.format(file_name = file_name), '{file_name}'.format(file_name = file_name))



def read_from_existing_vtt(bucket_name, file_name):
    vtt = WebVTT()
    blob = read_data_from_storage(bucket_name, file_name)
    blob = [string for string in blob.decode("utf-8").split('\n')[2:] if string]
    start, end = '', '' 
    for string in blob:
        if '-->' in string:
            start, end = string.split(' --> ')
        else:
            caption = Caption(start, end, string)
            vtt.captions.append(caption)

    return vtt



def read_from_existing_tsv(bucket_name, file_name, speech_transcription_with_speakertag):
    blob = read_data_from_storage(bucket_name, file_name)
    blob = blob.decode("utf-8")
    blob = [string for string in blob.split('\r\n') if string]
    with open('/tmp/{file_name}'.format(file_name = file_name), 'wt') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t')
        for string in blob:
            if len(string)>2:
                string = string.split('\t')
                tsv_writer.writerow([string[0], string[1], string[2].replace(' \n','')[1:-1]])
        
    upload_csv(speech_transcription_with_speakertag, '{file_name}'.format(file_name = file_name))



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
    
    if event['name'][-3:] == 'txt':
        write_data = json.loads(read_data_from_storage(event['bucket'], event['name']))
        video_file_path = write_data['video_file_path']
        
        cap = cv2.VideoCapture(generate_signed_url(video_file_path))
        fps = cap.get(cv2.CAP_PROP_FPS)      # OpenCV2 version 2 used "CV_CAP_PROP_FPS"
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if fps!=0 and frame_count!=0: duration = frame_count/fps
        else: duration = 3200
        print(duration)
                
        segment = types.VideoSegment()
        segment.start_time_offset.FromSeconds(int(duration//2)-10)
        segment.end_time_offset.FromSeconds(int(duration))
        result, speech_transcription_with_speakertag = transcribe_videosegments(video_file_path, segments = [segment])
        vtte = read_from_existing_vtt(event['bucket'], '{file_name}_subtitles.vtt'.format(file_name = event['name'][:-4]))
        transcribed_text, vtt = subtitle_generation(result, vtte, 4)

        with open("/tmp/{file_name}_subtitles.vtt".format(file_name = event['name'][:-4]), "w") as fd:
            vtt.write(fd)
        upload_blob("output_balance", '/tmp/{file_name}_subtitles.vtt'.format(file_name = event['name'][:-4]), '{file_name}_subtitles.vtt'.format(file_name = event['name'][:-4]))
        
        read_from_existing_tsv(event['bucket'], '{file_name}.tsv'.format(file_name = event['name'][:-4]), speech_transcription_with_speakertag)
        write_data['transcribed_text'] += transcribed_text
        with open('/tmp/{file_name}'.format(file_name = event['name']), 'w') as outfile:
            json.dump(write_data, outfile)
        
        upload_blob("transcribe_video_second_half", '/tmp/{file_name}'.format(file_name = event['name']), '{file_name}'.format(file_name = event['name']))

    