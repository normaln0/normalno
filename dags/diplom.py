
import datetime as dt
from datetime import timedelta
import os
from time import sleep
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import boto3
import pandahouse as ph
from dotenv import load_dotenv

load_dotenv() 

# environment variables
HOST = os.getenv("HOST") # IP
ACCSESS_KEY = os.getenv("ACCSESS_KEY") # AWS S3
SECRET_KEY = os.getenv("SECRET_KEY") 

# Connection to Ckickhouse
connection = {'host': 'http://{}:8123'.format(HOST),
                         'database': 'RIDES'}

# AWS S3
session = boto3.Session( 
         aws_access_key_id=ACCSESS_KEY, 
         aws_secret_access_key=SECRET_KEY)


bucket = 'bucket-project-net-08090'
new_bucket_name = 'bucket-project-net-0800090'
filename = ''
DATA = 'data/'

default_args = {
    'owner': 'airflow',
    'depend_on_past': True,
    'start_date': dt.datetime(2022, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Check files on bucket and download
def check_new_files_from_bucket():  
    list_ = os.listdir(DATA) 
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket)
    client = boto3.client('s3', aws_access_key_id=ACCSESS_KEY, aws_secret_access_key=SECRET_KEY)      
    for file in my_bucket.objects.all(): 
        filename = file.key  

        if filename not in list_:
            try:    
                client.download_file(Bucket = bucket, Key = filename, Filename = DATA + filename)                     
            except:
                print('File not found')
        else:
            continue

# Load file to ckickhouse
def load_big_file():
    list_ = os.listdir(DATA)

    for files in list_:
        try:
            df = pd.read_csv(DATA + files)
        except FileNotFoundError:
            print('File not found')

    df = df.rename(columns={'start station id' : 'start_station_id', 'start station name': 'start_station_name',
        'start station latitude': 'start_station_latitude', 'start station longitude':'start_station_longitude',
        'end station id':'end_station_id', 'end station name':'end_station_name', 'end station latitude':'end_station_latitude', 
        'end station longitude':'end_station_longitude', 'birth year':'birth_YEAR'})
    df = df.set_index('tripduration')

    ph.to_clickhouse(df, table='hits_buffer', connection=connection)
    
# number of trips per day
def transform_data_num_of_rides():

    list_ = os.listdir(DATA) 
    for files in list_: 
        try:
            df = pd.read_csv(DATA + files)
        except FileNotFoundError:
            print('File not found')

        df['start'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S')
        df['end'] = pd.to_datetime(df['stoptime'], format='%Y-%m-%d %H:%M:%S')
        df['date'] = df['start'].apply(lambda t: t.strftime('%Y-%m-%d')) 
        
        df = df.groupby('date')['bikeid'].agg('count').to_frame('number_rides') 

        ph.to_clickhouse(df, table='number_rides', connection=connection) 

        df.to_csv(DATA + 'new_data/' +  'transform_num_of_rides_per_day_' + files) 

# average trips per day
def transform_data_mean_ride():
    list_ = os.listdir(DATA) 

    for files in list_:
        try:
            df = pd.read_csv(DATA + files)
        except FileNotFoundError:
            print('File not found')
        
        df['start'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S')
        df['end'] = pd.to_datetime(df['stoptime'], format='%Y-%m-%d %H:%M:%S') 
        df['date'] = df['start'].apply(lambda t: t.strftime('%Y-%m-%d'))
        
        df['mean'] = df['end'] - df['start'] 
        df['mean'] = df["mean"].dt.total_seconds() 
        mean_ride = df.groupby('date')['mean'].agg('mean').to_frame('mean') 

        ph.to_clickhouse(mean_ride, table='mean_ride', connection=connection) 

        mean_ride.to_csv(DATA + 'new_data/' +  'transform_mean_ride_per_day_'+  files) 

# num of rides for «gender» category
def transform_data_gender_rides():
    list_ = os.listdir(DATA) 

    for files in list_:
        try:
            df = pd.read_csv(DATA + files)
        except FileNotFoundError:
            print('File not found')
            
        df['date'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S') 
        df['date'] = df['date'].dt.date 
        gender = df.groupby(['gender', 'date'])['tripduration'].agg('count') 

        gender = gender.to_frame('numbers').reset_index() 
        gender = gender.set_index('date') 

        ph.to_clickhouse(gender, table='gender', connection=connection) 
        
        gender.to_csv(DATA + 'new_data/' +  'transform_gender_values_' + files)  


# Load reports to S3 bucket
def upload_new_files_to_bucket():
    s3 = boto3.client('s3', aws_access_key_id=ACCSESS_KEY, aws_secret_access_key=SECRET_KEY) 
    list_ = os.listdir(DATA + 'new_data/') 
    for file in list_:
        s3.upload_file(
        Filename= (DATA + 'new_data/' +   file), 
        Bucket=new_bucket_name, 
        Key = file) 

# create new bucket if not exist
def create_buckets_if_not_exist():
    try:
        s3 = boto3.resource('s3', aws_access_key_id=ACCSESS_KEY, aws_secret_access_key=SECRET_KEY)
        bucket = s3.Bucket(new_bucket_name)
        response = bucket.create(CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    except:
        print('Buckets exist')

# remove files
def remove_files_from_os():
    list_ = os.listdir(DATA) 
    list_2 = os.listdir(DATA + 'new_data/') 
    for file in list_: 
        try:
            os.remove(DATA + file) 
        except:
            continue
    for file in list_2:
        try:
            os.remove(DATA + 'new_data/'+ file)
        except:
            continue

# number of trips by year of birth in the first 5 days of the month
def example_select():
    
    query = """ SELECT  birth_YEAR, toDate(starttime) as new_date, COUNT(tripduration)
    from RIDES.rides_to_new_york rtny
    group by birth_YEAR, new_date
    HAVING new_date BETWEEN  '2014-01-01' AND '2014-01-05'
    order by birth_YEAR 
    """

    new_df = ph.read_clickhouse(query, index='tripduration', connection=connection)
    new_df = new_df.set_index('birth_YEAR')
    new_df.to_csv(DATA + 'new_data/' +  'example_select.csv') 

# if files not exist
def to_transform(**context):
    files = os.listdir(DATA) 

    if len(files) != 0:
        return ['trans_1', 'trans_2', 'trans_3']

    return 'no_files'

   
def files_not_exist():
    print('Do not have any files')


with DAG ("pipline_csv", default_args=default_args, template_searchpath= DATA, schedule_interval='@daily') as dag:
    check_download = PythonOperator(task_id='download', python_callable= check_new_files_from_bucket) 
    transform1 = PythonOperator(task_id='trans_1', python_callable=transform_data_num_of_rides) 
    transform2 = PythonOperator(task_id='trans_2', python_callable=transform_data_mean_ride) 
    transform3 = PythonOperator(task_id='trans_3', python_callable=transform_data_gender_rides) 
    create_buckets = PythonOperator(task_id='create_buk', python_callable=create_buckets_if_not_exist) 
    upload = PythonOperator(task_id='upload_to_buk', python_callable=upload_new_files_to_bucket)
    remove = PythonOperator(task_id='rem_files', python_callable=remove_files_from_os)
    to_trans = BranchPythonOperator(task_id='trans_tree', provide_context=True, python_callable=to_transform)
    not_f = PythonOperator(task_id='no_files', python_callable=files_not_exist)
    big_file = PythonOperator(task_id='load_big_file', python_callable=load_big_file)
    example = PythonOperator(task_id='example', python_callable=example_select)

# more details in the README file schema
    check_download >> to_trans  
    to_trans >> big_file >> [transform2, transform3, transform1] >> example >> create_buckets >> upload >> remove
    to_trans >> not_f