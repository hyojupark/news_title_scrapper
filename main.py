from datetime import datetime, timedelta
import os
import requests
import re
import json
import io

from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from fastavro import writer, parse_schema


def clean_html(raw_html: str) -> str:
    cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    clean_text = re.sub(cleanr, '', raw_html)
    return clean_text


def get_news_data():
    # credentials = service_account.Credentials.from_service_account_file('credentials/news-analysis_service-account.json')
    # naver_api_credentials = json.load(open('credentials/hyoju_naver_search_api.json'))

    naver_api_credentials = {
        'id': os.environ['NAVER_SEARCH_API.ID'],
        'secret': os.environ['NAVER_SEARCH_API.SECRET']
    }

    headers = {
        'X-Naver-Client-Id': naver_api_credentials['id'],
        'X-Naver-Client-Secret': naver_api_credentials['secret']
    }

    params = {
        'query': '기자',
        'display': 100,
        'start': 1,
        'sort': 'date'
    }

    return requests.get('https://openapi.naver.com/v1/search/news.json', headers=headers, params=params).json()


def run(request):
    end_date = datetime.now().replace(second=0, microsecond=0)
    start_date = end_date - timedelta(minutes=1)

    schema = {
        'doc': 'news_data',
        'name': 'news_data',
        'namespace': 'news_data',
        'type': 'record',
        'fields': [
            {'name': 'title', 'type': 'string'},
            {'name': 'description', 'type': 'string'},
            {'name': 'originallink', 'type': 'string'},
            {'name': 'link', 'type': 'string'},
            {'name': 'pubDate', 'type': {'type': 'long', 'logicalType': 'timestamp-millis'}}
        ]
    }
    parsed_schema = parse_schema(schema)

    resp = get_news_data()

    df = pd.DataFrame(resp['items'])
    df['title'] = df['title'].apply(lambda x: clean_html(x))
    df['description'] = df['description'].apply(lambda x: clean_html(x))
    
    df['pubDate'] = df['pubDate'].apply(lambda x: datetime.strptime(x, "%a, %d %b %Y %H:%M:%S %z").timestamp() * 1000)
    filtered_df = df.loc[(df['pubDate'] >= start_date.timestamp() * 1000)
                         & (df['pubDate'] < end_date.timestamp() * 1000)]
    print(filtered_df)

    if not filtered_df.empty:
        # credentials = service_account.Credentials.from_service_account_file('credentials/news-analysis_service-account.json')
        # client = storage.Client(project=credentials.project_id)
        client = storage.Client(project=os.environ['PROJECT.ID'])
        bucket = client.get_bucket(os.environ['BUCKET.NAME'])

        bytesio = io.BytesIO()
        writer(bytesio, parsed_schema, filtered_df.to_dict('records'))
        start_date_str = start_date.strftime('%Y%m%d%H%M%S')
        end_date_str = end_date.strftime('%Y%m%d%H%M%S')
        bucket.blob(f'raw_data/{start_date_str}_{end_date_str}.avro').upload_from_file(bytesio, rewind=True)

    return {'status': 'success'}
