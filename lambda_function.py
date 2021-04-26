import os
import copy
import datetime
import requests
import time
import logging
import zipfile
import json
import pandas as pd
import boto3
import base64
from urllib.parse import unquote
import io


# Internal settings: 

max_tries = 200  # Controls how many time reports are regenerated and redownloaded before erroring

# PATH: S3://{bucket}/{folder}/utterance_transcribed/example.json
bucket = 'bucket'  # Bucket that utterances get hosted to

# PATH S3://{bucket}/{path}/
job_folder = 'source_jobs/dev' # Folder in bucket to store source job_ids 
results_header = 'tx_work' # annotation results header from origin job


# Initializes logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initializes S3 Success Access
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')  

def parse_event(event: str) -> dict:
    '''
    Parses signal, payload, and signature from a figure eight
    webhook query string
    :param event: query string received from figure eight webhook
    :return: dictionary with keys: signal, payload, and signature
    '''
    if 'source' in event:
        return {'signal':'timer'}
    event_body = base64.b64decode(event['body']).decode('utf-8')
    signal = event_body.split('&')[0].replace("signal=", "")
    payload = json.loads(unquote(event_body.split('&')[1].replace("payload=", "")))
    signature = event_body.split('&')[2].replace("signature=", "")

    return {
        'signal': signal,
        'payload': payload,
        'signature': signature
    }
    
def get_job_title(job_id,params):
    response = requests.get(
        f'https://api.appen.com/v1/jobs/{job_id}.json', params=params)
    if response.status_code != 200:
        print(
            f'---- Status code: {response.status_code} \n {response.text}')
    else:
        print('---- Success!')
        read_response = json.loads(response.text)
        return (read_response['title'])

def get_anno_path(s3_path):
    print(s3_path)
    bucket = s3_path.split('/')[2]
    filepath = "/".join(s3_path.split('/')[3:])
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=filepath)
    except:
        return False
    s3_clientdata = obj['Body'].read().decode('utf-8')
    return s3_clientdata
    
def get_anno_url_old(anno_url):
    response = requests.get(anno_url, verify=False)
    anno_transcribed = response.json()
    return anno_transcribed
    
def get_anno_url(obj):
    if '{' in obj:
        print(obj)
        json_obj = json.loads(obj)
        print(json_obj)
        url = json_obj['url']
        print(url)
        anno_url = url.replace('requestor-proxy.appen.com','api-beta.appen.com') + f"&key={os.environ['API_KEY']}"
    else:
        anno_url = obj
    response = requests.get(anno_url, verify=False)
    anno_transcribed = response.json()
    return anno_transcribed

def get_utts(row,row_list):
    '''
    Extracts utterances from annotation
    '''
    worker = row['_worker_id']
    if not row['anno0']:
        return 
    anno0 = json.loads(row['anno0'])
    anno1 = row['anno1']
    audio_annotation_url = row['audio_annotation_url']
    audio_url = row['audio_url']
    unit_id = row['_unit_id']
    created_at = row['_created_at']
    if anno1['nothingToTranscribe'] or not len(anno1['annotation']):
        return
    print('annotation', anno1['annotation'])
    for utt1 in anno1['annotation'][0]:
        if utt1['nothingToTranscribe']:
            continue
        sample1 = {'annotation':[[utt1]],"nothingToAnnotate": False, "ableToAnnotate": True, "nothingToTranscribe": False }
        for utt0 in anno0['annotation']:
            if utt0['id'] == utt1['id']:
                sample0 = {'annotation':[[utt0]],"nothingToAnnotate": False }
        row_dict = {'sample0':sample0,
                    'sample1':sample1,
                    'orig_worker_id':worker,
                    'audio_annotation_url':audio_annotation_url,
                    'audio_url':audio_url,
                    'orig_unit_id':unit_id,
                    'orig_created_at' : created_at,
                    'display_id':row['display_id'],
                    'duration':row['duration'],
                    'pe_file_id':row['pe_file_id'],
                    'pe_file_name':row['pe_file_name'],
                    'pe_store_id':row['pe_store_id']
        }
        row_list.append(row_dict)

def host_utts(row, bucket, job_id):
    '''
    Hosts utterances to s3 bucket
    '''
    print('bucket',bucket)
    print('job_id',job_id)
    audio_annotation_url = row['audio_annotation_url']
    print(audio_annotation_url)
    folder = "/".join(audio_annotation_url.split('/')[3:-1])
    filename = audio_annotation_url.split('/')[-1].replace('.json','')
    sample0 = row['sample0']
    sample1 = row['sample1']
    audio_url = row['audio_url']
    worker = row['orig_worker_id']
    unit_id = row['orig_unit_id']
    sample_id = sample0['annotation'][0][0]['id']
    utterance = f'QL1/QA/{job_id}/utterance/{folder}/{filename}_{sample_id}.json'
    utterance_transcribed = f'QL1/QA/{job_id}/utterance_transcribed/{folder}/{filename}_{sample_id}_{worker}.json'
    s3.Object(bucket, utterance).put(Body=json.dumps(sample0))
    s3.Object(bucket, utterance_transcribed).put(Body=json.dumps(sample1))
    utterance_path = f's3://{bucket}/{utterance}'
    utterance_transcribed_path = f's3://{bucket}/{utterance_transcribed}'
    row_dict = {'utterance': utterance_path, 'utterance_transcribed': utterance_transcribed_path}
    return pd.Series(row_dict)

# sample utterances with a minimum of 1/contributor
def sample(chunk, rate):
    n = max(int(len(chunk)*rate), 1)
    return chunk.sample(n=n, replace=True, random_state=1)


def put_job_id(job_id,bucket,job_folder):
    s3.Object(bucket, job_folder + '/' + job_id).put(Body='')
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=job_folder)
    for obj in response.get('Contents', []):
        print('Found object', obj)
        if obj['Key'] == job_folder + '/' + job_id:
            return False
    print(f'Added {job_id} as a source job!')
    return True
    
def get_job_ids(bucket,job_folder):
    jobs = []
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=job_folder)
    for obj in response.get('Contents', []):
        print(obj)
        jobs.append(obj['Key'].split('/')[-1].split('.')[0])
        print(jobs)
    return jobs
        


def regenerate_report(job_id, params, max_tries):
    '''
    Takes in a job ID, param {'key': my_api_key, 'type': reporttype}, and max_tries
    '''
    counter = 0
    while counter < max_tries:
        response = requests.post(f'https://api.appen.com/v1/jobs/{job_id}/regenerate', params=params)
        counter += 1
        print(f'-- Response: {response.status_code} -- {str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))} -- Count:{counter}')
        if response.status_code == 200:
            print(f'Regenerated {job_id} {params[1][1]} report!')
            break
        time.sleep(1)
    return True

def get_report(job_id, params, max_tries):
    '''
    Takes in a job ID, param {'key': my_api_key, 'type': reporttype}, and max_tries
    '''
    counter = 0
    while counter < max_tries:
        response = requests.get(f'https://api.appen.com/v1/jobs/{job_id}.csv', params=params)
        counter += 1
        print(f'-- Response: {response.status_code} -- {str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))} -- Count:{counter}')
        if response.status_code == 200:
            print(f'Download complete for job {job_id}, Saving {params[1][1]} report')
            break
        time.sleep(1)
    if params[1][1] == 'full':
        fname = f'f{job_id}'
    elif params[1][1] == 'source':
        fname = f'source{job_id}'
    output = open(f'/tmp/{fname}.zip', 'wb')
    output.write(response.content)
    output.close()
    print(f'-- Extracting {fname}.zip')
    try:
        zf = zipfile.ZipFile(f'/tmp/{fname}.zip')
        print('UNZIP REPORT')
    except:
        print('UNABLE TO UNZIP')
    df = pd.read_csv(zf.open(f'{fname}.csv'))
    print(f'reading the {params[1][1]} report')
    return df

def upload_report(df, job_id):
    '''
    Takes in a DataFrame and job ID
    '''
    df.to_csv('/tmp/qa_src.csv', index=False)
    with open('/tmp/qa_src.csv') as csvfile:
        source = csvfile.read()
    headers = {"Content-Type": "text/csv"}
    req = f"https://api.appen.com/v1/jobs/{job_id}/upload.json?key={os.environ['API_KEY']}"
    res = requests.post(req, headers=headers,  data=source)
    return res
    
def job_handler(job_1):
   
    params_1 = (
    ('key', os.environ['API_KEY']),
    ('type', 'full'),
    )
    regenerate_report(job_1,params_1,max_tries)
    df1 = get_report(job_1,params_1,max_tries)
    
    if len(df1) == 0:
        logger.info(f'No rows in origin job {job_1}!')
        return False
    max_i = len(df1)-1
    try:
        job_2 = df1['qa_job'][max_i]
    except Exception as e:
        logger.info(f'Could not get QA Job ID from {job_1}!')
        logger.info(e)
        return False
    try:
        sample_pct = df1['sample'][max_i]
    except Exception as e:
        logger.info(f'Could not get sample rate from {job_1}!')
        logger.info(e)
        return False
    
    params_2 = (
    ('key', os.environ['API_KEY']),
    ('type', 'source'),
    )
    regenerate_report(job_2,params_2,max_tries)
    df2 = get_report(job_2,params_2,max_tries)
    
    if len(df2) != 0:
        timestamp = pd.to_datetime(df2['orig_created_at']).max()
        logger.info(f'most recent timestamp in QA job: {str(timestamp)}')
        df1['_created_at_dt'] = pd.to_datetime(df1['_created_at'])
        df1 = df1[df1['_created_at_dt'] > timestamp]
        df1.sort_values(by='_created_at_dt')
        df1 = df1.head(250)
        if len(df1) == 0:
            logger.info('No new rows to sample!')
            return True
    else:
        logger.info(f'No rows in QA Job {job_2}! ')
    
    df1['anno1'] = df1[results_header].apply(get_anno_url)
    
    # filter out rows with no audio_transcription
    df1 = df1[df1['anno1']!={'annotation': [], 'nothingToAnnotate': False, 'ableToAnnotate': False, 'nothingToTranscribe': True}]
    
    df1['anno0'] = df1['audio_annotation_url'].apply(get_anno_path)
    
    row_list = []
    df1.apply(lambda x: get_utts(x,row_list),axis=1)
    df = pd.DataFrame(row_list)
    if len(df) == 0:
        print('All new utterances were marked as "Nothing to Annotate"')
        return True
    # Divided by 2 since it was oversampling
    df = df.groupby('orig_worker_id').apply(lambda x: sample(x, sample_pct))
    # df = df.apply(lambda x: x.sample(frac=sample_pct))
    df[['utterance', 'utterance_transcribed']] = df.apply(lambda x: host_utts(x, bucket, job_1), axis=1)
    df['orig_job_id'] = job_1
    print(df.columns)
    df = df.drop(columns=['sample0','sample1']) 
    params = {'key': os.environ['API_KEY']}
    df['from_job_name'] = get_job_title(job_1,params)
    
    df_len = len(df)
    print(f'Uploading {df_len} rows to {job_2}')
    res = upload_report(df, job_2)
    logger.info(res)
    
    return True
    
    
def lambda_handler(event, context):
    
    print(event)
    event = parse_event(event)
    print(event)
    if event['signal'] == 'timer':
        # Get origin job IDs
        jobs = get_job_ids(bucket,job_folder)
        print(jobs[0],jobs)
        for job in jobs:
            print(job)
            job_status = job_handler(job)
            if not job_status:
                print(f'Something went wrong with {job}!')
        return {'statusCode': 200}
    else:
        try:
            job_id = str(event['payload']['job_id'])
        except:
            job_id = str(event['payload'][0]['job_id'])
        put_job_id(job_id,bucket,job_folder)
        return {'statusCode': 200}
    