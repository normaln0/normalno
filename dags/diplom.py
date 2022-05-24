
"""
1. AWS API ключи необходимо убрать из общего доступа, нужно иди Variables , или подгружать ,через локальный env. 
В таком виде хранить их небезопасно, как и данные доступа к БД

---- Добавил переменные окружения, убрал данные из общего доступа. ----------

2. Если пользуетесь docker-compose для развёртывания, хорошо бы добавить описание как запускается и для чего файлы конфигурации(airflow.cfg, entrypoint.sh). 
В противном случае, лучше убрать из проекта . Также непонятна папка diplom,возможно при коммите случайно попала

--------- Файлы airflow.cfg, entrypoint.sh убрал. --------------------
--------- папка диплом попала случайно, убрал ----------------
--------- В README добавил описание docker-compose ----------------

3. if filename not in list_:
try:
client.download_file(Bucket = bucket, Key = filename, Filename = DATA + filename)
Не очень понятна логика** if not**, возможно наоборот имеется ввиду?

--------- Нет, тут все правильно, логика (if not in): если такого файла в папке нет - скачать файл. Если такой файл уже есть -continue---------------------------------
--------- Дважды перепроверил - работает

Для более быстрого понимания, добавьте пожалуйста описание аргументов каждой функции , и что она возвращает 

---------- Описание добавил -------------------
													
4. Не очень пока понимаю, при каких условиях размер файлов нулевой будет

for file in list_:
if len(file) == 0:

--------- Да, какая-то чушь, ошибся, поправил -----------------------

5. Данные в clickhouse также можно загрузить путем SQL запросов , как в README написано, неплохо это в dag файле указать, плюс можно отчеты также в репозиторий загрузить

------- Данные и так загружаются в clickhouse, информация есть на скрине. ----------------------
------- У pandahouse нет возможности INSER тить файлы запросами, только читать https://github.com/kszucs/pandahouse ----------------
------- сначала я пытался загрузить через ckickhouse-driver, но возникли проблемы с 9000 портом, мой ноутбук отказывается его открывать --------------
------- много искал информацмию по этому поводу, пишут, что на windows такая проблема существует-----------------
------- Но, для примера, чтобы показать SQL запросы в коде, я добавил  функцию def example_select() и сохранил в csv файл, который приложил к остальным в папке example
------- отчеты загрузил -----------------

Также будет здорово, если добавите дизайн-схему проекта , с указанием все сервисов и их интеграции

------ Схему добавил в README --------------------------

"""

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

load_dotenv() # Забираем переменные окружения из файла .env

HOST = os.getenv("HOST") # мой IP
ACCSESS_KEY = os.getenv("ACCSESS_KEY") # логин к AWS
SECRET_KEY = os.getenv("SECRET_KEY") # пароль к AWS

# Подключение к Ckickhouse
connection = {'host': 'http://{}:8123'.format(HOST),
                         'database': 'RIDES'}

# Ключ и пароль S3 (connection_bases)
session = boto3.Session( 
         aws_access_key_id=ACCSESS_KEY, 
         aws_secret_access_key=SECRET_KEY)

# Название бакета для загрузки первичных файлов
bucket = 'bucket-project-net-08090'
new_bucket_name = 'bucket-project-net-0800090'
filename = ''
DATA = 'data/' # директория скачинвания файлов

default_args = {
    'owner': 'airflow',
    'depend_on_past': True,
    'start_date': dt.datetime(2022, 5, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Проверка файлов на бакете S3 и их загрузка на пк.
def check_new_files_from_bucket():  
    list_ = os.listdir(DATA)  # Список файлов в папке
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket)
    client = boto3.client('s3', aws_access_key_id=ACCSESS_KEY, aws_secret_access_key=SECRET_KEY)      
    for file in my_bucket.objects.all(): # Проверяет все файл на бакете
        filename = file.key   # название файла

        if filename not in list_: # Если файла с таким названием нет в папке 
            try:    
                  client.download_file(Bucket = bucket, Key = filename, Filename = DATA + filename)   # скачивает файл в папку data                        
            except:
                print('Files not found in bucket S3')
        else:
            continue

# Загрузка основного файла на ckickhouse
def load_big_file():
    list_ = os.listdir(DATA) # Список файлов в папке

    for files in list_: # итерируем все файлы в папке
        try:
            df = pd.read_csv(DATA + files) # читаем новый файл в папке.
        except:
            continue

# перемеиновываем колонки
    df = df.rename(columns={'start station id' : 'start_station_id', 'start station name': 'start_station_name',
        'start station latitude': 'start_station_latitude', 'start station longitude':'start_station_longitude',
        'end station id':'end_station_id', 'end station name':'end_station_name', 'end station latitude':'end_station_latitude', 
        'end station longitude':'end_station_longitude', 'birth year':'birth_YEAR'})
# Убираем лишнюю колонку с индексами
    df = df.set_index('tripduration')
# загружаем данные в clickhouse
    ph.to_clickhouse(df, table='hits_buffer', connection=connection)
    
# количество поездок в день
def transform_data_num_of_rides():
# Список файлов в папке
    list_ = os.listdir(DATA) 
    for files in list_: # итерируем все файлы в папке
        try:
            df = pd.read_csv(DATA + files)
        except:
            continue

        df['start'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S') # преобразовываем в datetime
        df['end'] = pd.to_datetime(df['stoptime'], format='%Y-%m-%d %H:%M:%S') # преобразовываем в datetime
        df['date'] = df['start'].apply(lambda t: t.strftime('%Y-%m-%d')) # Выводим дату
        
        df = df.groupby('date')['bikeid'].agg('count').to_frame('number_rides') # группируем по дате, количество поездок. ПРриводим к датафрейму

        ph.to_clickhouse(df, table='number_rides', connection=connection) # подгружаем в ckickhouse

        df.to_csv(DATA + 'new_data/' +  'transform_num_of_rides_per_day_' + files) # создаем файл csv

# средняя продолжительность поездок в день
def transform_data_mean_ride():
    list_ = os.listdir(DATA) 

    for files in list_:
        try:
            df = pd.read_csv(DATA + files)
        except:
            continue
        
        df['start'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S') # преобразовываем в datetime
        df['end'] = pd.to_datetime(df['stoptime'], format='%Y-%m-%d %H:%M:%S') # преобразовываем в datetime
        df['date'] = df['start'].apply(lambda t: t.strftime('%Y-%m-%d')) # Выводим дату
        
        df['mean'] = df['end'] - df['start'] # колонка с разностью окончания поедки и начала поездки
        df['mean'] = df["mean"].dt.total_seconds() # находим продолжительность поездки
        mean_ride = df.groupby('date')['mean'].agg('mean').to_frame('mean') # группируем по дате среднюю продолжительность поездки

        ph.to_clickhouse(mean_ride, table='mean_ride', connection=connection) # подгружаем в ckickhouse

        mean_ride.to_csv(DATA + 'new_data/' +  'transform_mean_ride_per_day_'+  files)  # создаем файл csv    


# распределение поездок пользователей, разбитых по категории «gender»  
def transform_data_gender_rides():
    list_ = os.listdir(DATA) 

    for files in list_:
        try:
            df = pd.read_csv(DATA + files)
        except:
            continue
            
        df['date'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S') # преобразовываем в datetime
        df['date'] = df['date'].dt.date # колонку date приводим к формату yyyy--mm--dd
        gender = df.groupby(['gender', 'date'])['tripduration'].agg('count') # группируем по gender и date количество поездок

        gender = gender.to_frame('numbers').reset_index() # приводим к фрейму, убираем индексы
        gender = gender.set_index('date') 

        ph.to_clickhouse(gender, table='gender', connection=connection) # подгружаем в ckickhouse
        
        gender.to_csv(DATA + 'new_data/' +  'transform_gender_values_' + files) # создаем файл csv   


# Загружаем отчеты в новый бакет
def upload_new_files_to_bucket():
    s3 = boto3.client('s3', aws_access_key_id=ACCSESS_KEY, aws_secret_access_key=SECRET_KEY) 
    list_ = os.listdir(DATA + 'new_data/') 
    for file in list_:
        s3.upload_file(
        Filename= (DATA + 'new_data/' +   file), # путь откуда загружаем
        Bucket=new_bucket_name, # название бакета
        Key = file) # сам файл

# создаем новый бакет, если он не существует
def create_buckets_if_not_exist():
    try:
        s3 = boto3.resource('s3', aws_access_key_id=ACCSESS_KEY, aws_secret_access_key=SECRET_KEY)
        bucket = s3.Bucket(new_bucket_name)
        response = bucket.create(CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
    except:
        print('Buckets exist')

# Удаляем преобразованные и загруженные файлы с пк в конце
def remove_files_from_os():
    list_ = os.listdir(DATA) 
    list_2 = os.listdir(DATA + 'new_data/') 
    for file in list_: # удаляем из корневой папки
        try:
            os.remove(DATA + file) # удаляем из вложенной папки
        except:
            continue
    for file in list_2:
        try:
            os.remove(DATA + 'new_data/'+ file)
        except:
            continue

# В качестве примерв работы с SQL запросами 
def example_select():
    # Находим количество поездок по году рождения за первые 5 дней месяца
    query = """ SELECT  birth_YEAR, toDate(starttime) as new_date, COUNT(tripduration)
    from RIDES.rides_to_new_york rtny
    group by birth_YEAR, new_date
    HAVING new_date BETWEEN  '2014-01-01' AND '2014-01-05'
    order by birth_YEAR 
    """

    new_df = ph.read_clickhouse(query, index='tripduration', connection=connection)
    new_df = new_df.set_index('birth_YEAR')

    new_df.to_csv(DATA + 'new_data/' +  'example_select.csv') # сохраняем в csv файл

# Проверка существования файлов
# Находим список файлов в папке, если список не пуст возвращаем функции трансформации
# Если список пуст возвращаем функцию выводящую информацию об отсутсвии файлов
def to_transform(**context):
    files = os.listdir(DATA) 

    if len(files) != 0:
        return ['trans_1', 'trans_2', 'trans_3']

    return 'no_files'

# при отсутсвии файлов    
def files_not_exist():
    print('Do not have any files')


with DAG ("pipline_csv", default_args=default_args, template_searchpath= DATA, schedule_interval='@daily') as dag:
    check_download = PythonOperator(task_id='download', python_callable= check_new_files_from_bucket) # отслеживает файлы в бакете
    transform1 = PythonOperator(task_id='trans_1', python_callable=transform_data_num_of_rides) # трнсформация
    transform2 = PythonOperator(task_id='trans_2', python_callable=transform_data_mean_ride) # трнсформация
    transform3 = PythonOperator(task_id='trans_3', python_callable=transform_data_gender_rides) # трнсформация
    create_buckets = PythonOperator(task_id='create_buk', python_callable=create_buckets_if_not_exist) 
    upload = PythonOperator(task_id='upload_to_buk', python_callable=upload_new_files_to_bucket)
    remove = PythonOperator(task_id='rem_files', python_callable=remove_files_from_os)
    to_trans = BranchPythonOperator(task_id='trans_tree', provide_context=True, python_callable=to_transform)
    not_f = PythonOperator(task_id='no_files', python_callable=files_not_exist)
    big_file = PythonOperator(task_id='load_big_file', python_callable=load_big_file)
    example = PythonOperator(task_id='example', python_callable=example_select)


   
# подробнее в схеме файла README
    check_download >> to_trans  
    to_trans >> big_file >> [transform2, transform3, transform1] >> example >> create_buckets >> upload >> remove
    to_trans >> not_f