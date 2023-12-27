from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import requests
import io
import zipfile
from mimetypes import guess_type

@task
def download_rs_pbp_to_s3(year: str):
    url = f'https://www.retrosheet.org/events/{year}eve.zip'
    s3_bucket = 'jamesmedaugh'
    s3_file = f'BBDB/rs-pbp/zips/{year}/{year}eve.zip'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()

    response = requests.get(url)
    response.raise_for_status()

    s3_client.put_object(Bucket=s3_bucket, Key=s3_file, Body=response.content)

    return s3_file

@task
def unzip_to_s3(year: str, s3_file: str):
    s3_bucket = 'jamesmedaugh'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()

    zip = s3_client.get_object(Bucket=s3_bucket, Key=s3_file)
    buffer = io.BytesIO(zip['Body'].read())

    with zipfile.ZipFile(buffer, mode='r') as zipf:
        for info in zipf.infolist():
            file_path = f'BBDB/rs-pbp/unzipped/{year}/{info.filename}'
            with zipf.open(info) as file:
                s3_client.upload_fileobj(file, Bucket=s3_bucket, Key=file_path)

    return f'BBDB/rs-pbp/unzipped/{year}/'

@task
def move_event_files_in_s3(year: str, unzip_path: str):
    s3_bucket = 'jamesmedaugh'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()

    files = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=unzip_path)
    files_to_split = []

    for file in files.get('Contents', []):
        file_path = file['Key']
        new_path = f'BBDB/rs-pbp/event-files/{year}/'+file_path.split('/')[-1]
        if '.EVN' in file_path or '.EVA' in file_path:
            s3_client.copy_object(Bucket=s3_bucket, CopySource={'Bucket': s3_bucket, 'Key': file_path}, Key=new_path)
            files_to_split.append(new_path)

    return files_to_split

@task
def move_roster_files_in_s3(year: str, unzip_path: str):
    s3_bucket = 'jamesmedaugh'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()

    files = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=unzip_path)

    for file in files.get('Contents', []):
        file_path = file['Key']
        new_path = f'BBDB/rs-pbp/roster-files/{year}/'+file_path.split('/')[-1]
        if '.ROS' in file_path:
            s3_client.copy_object(Bucket=s3_bucket, CopySource={'Bucket': s3_bucket, 'Key': file_path}, Key=new_path)

@task
def move_team_files_in_s3(year: str, unzip_path: str):
    s3_bucket = 'jamesmedaugh'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()

    files = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=unzip_path)

    for file in files.get('Contents', []):
        file_path = file['Key']
        new_path = f'BBDB/rs-pbp/team-files/{year}/'+file_path.split('/')[-1]
        if 'TEAM{year}' in file_path:
            s3_client.copy_object(Bucket=s3_bucket, CopySource={'Bucket': s3_bucket, 'Key': file_path}, Key=new_path)

@task
def split_event_files_in_s3(year: str, file: str):
    s3_bucket = 'jamesmedaugh'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()

    event_file = s3_client.get_object(Bucket=s3_bucket, Key=file)
    event_data = event_file['Body'].read().decode('utf-8')

    games = {}
    current_game_id = None
    for line in event_data.splitlines():
        if line.startswith('id,'):
            current_game_id = line.split(',')[1].strip()
            games[current_game_id] = []
        if current_game_id is not None:
            games[current_game_id].append(line)

    for game_id, lines in games.items():
        game_data = '\n'.join(lines)
        file_name = f'BBDB/rs-pbp/game-files/{year}/{game_id}.txt'
        s3_client.put_object(Bucket=s3_bucket, Key=file_name, Body=game_data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


@dag(
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 12, 1),
        catchup=False,
        params={'year':'2023'},
)
def retrosheet_downloader():
    year = '{{ params.year }}'
    s3_file = download_rs_pbp_to_s3(year=year)
    unzip_path = unzip_to_s3(year=year, s3_file=s3_file)

    move_roster_files_in_s3(year=year, unzip_path=unzip_path)
    move_team_files_in_s3(year=year, unzip_path=unzip_path)

    split_event_files_in_s3.partial(year=year).expand(file=move_event_files_in_s3(year=year, unzip_path=unzip_path))


dag = retrosheet_downloader()

