from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import json
from io import BytesIO

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    # Parse the stock data if it's a JSON string
    if isinstance(stock, str):
        stock = json.loads(stock)

    # Set up the S3 connection using the S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_s3')
    bucket_name = 'codingmatte'  # Root bucket name
    
    # Prepare the full path and file structure
    symbol = stock['meta']['symbol']
    s3_key = f'data/ingestion/airflow_stock_prices/{symbol}/prices.json'

    # Prepare data to upload
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    
    # Upload data to S3
    s3_hook.load_bytes(
        bytes_data=BytesIO(data).read(),  # Load data as bytes
        key=s3_key,                       # S3 key with desired path
        bucket_name=bucket_name,
        replace=True                      # Overwrite if file exists
    )

    # Return the S3 path of the stored object
    return f's3://{bucket_name}/{s3_key}'

def _store_prices_minio(stock):
    minio = BaseHook.get_connection('minio').extra_dejson
    client = Minio(
        endpoint=minio['endpoint_url'].split('//')[1],
        access_key=minio['aws_access_key_id'],
        secret_key=minio['aws_secret_access_key'],
        secure=False
    )
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')

    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'
    