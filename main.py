from datetime import datetime, timedelta
import os
import requests
import re
import json
import io

from google.cloud import storage
from google.oauth2 import service_account
# from google.cloud import bigquery
# from google.oauth2 import service_account
import pandas as pd
from fastavro import writer, parse_schema


def clean_html(raw_html: str) -> str:
    cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    clean_text = re.sub(cleanr, '', raw_html)
    return clean_text


def get_news_data():
    # credentials = service_account.Credentials.from_service_account_file('credentials/hyoju_service_account.json')
    # client = bigquery.Client(credentials=credentials, project=credentials.project_id)

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


def news_data_save_to_gcs(request):
    end_date = datetime.utcnow() + timedelta(hours=9)
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

    # credentials = service_account.Credentials.from_service_account_file('credentials/hyoju_service_account.json')
    # client = storage.Client(project=credentials.project_id)
    client = storage.Client(project=os.environ['PROJECT.ID'])
    bucket = client.get_bucket(os.environ['BUCKET.NAME'])

    bytesio = io.BytesIO()
    writer(bytesio, parsed_schema, filtered_df.to_dict('records'))
    start_date_str = start_date.strftime('%Y%m%d%H%M%S')
    end_date_str = end_date.strftime('%Y%m%d%H%M%S')
    bucket.blob(f'raw_data/{start_date_str}_{end_date_str}.avro').upload_from_file(bytesio, rewind=True)

    return {'status': 'success'}



# print(type(resp['items'][0]['pubDate']), resp['items'][0]['pubDate'])
# pub_date_dt = datetime.strptime(resp['items'][0]['pubDate'], "%a, %d %b %Y %H:%M:%S %z")
# print(type(pub_date_dt), pub_date_dt)

# print(resp.status_code)
# print(resp.json())

# df = pd.DataFrame()
# news_list = []
# for news in resp['items']:
#     title = news['title']
#     description = news['description']
#     original_link = news['originallink']
#     link = news['link']
#     pub_date = news['pubDate']

#     print('-'*50)
#     print(title)
#     print(description)
#     print(original_link)
#     print(link)
#     print(pub_date)

#     news_list.append({'title': title, 'description': description, 'original_link': original_link, 'link': link, 'pub_date': pub_date}, ignore_index=True)

# df = pd.DataFrame(resp['items'])
# print(df)
# df['title'] = df['title'].apply(lambda x: clean_html(x))
# df['description'] = df['description'].apply(lambda x: clean_html(x))
# df['pubDate'] = df['pubDate'].apply(lambda x: datetime.strptime(x, "%a, %d %b %Y %H:%M:%S %z"))
# print(df)

# table_id = 'hyoju-387406.naver_news.search_list_v2'
# job = client.load_table_from_dataframe(df, table_id)
# job.result()