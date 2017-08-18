import argparse
import collections
import csv
import datetime
import io
import json
import logging
import os
import zipfile

import psycopg2
from psycopg2 import sql
import requests
from requests.auth import HTTPBasicAuth

DataSetMetadata = collections.namedtuple('DataSetMetadata', ['plugin', 'table'])

logger = logging.getLogger(__name__)

API_VERSION = '1.18'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config.json'

FULL_DATA_SET_METADATA = [
    DataSetMetadata(
        plugin='07a9e561-e22f-4e82-8dd6-7bfb14c91776',
        table='org_units'
    ),
    DataSetMetadata(
        plugin='793668a8-2c58-4e5e-b263-412d28d5703f',
        table='grade_objects'
    ),
    DataSetMetadata(
        plugin='1d6d722e-b572-456f-97c1-d526570daa6b',
        table='users'
    ),
    DataSetMetadata(
        plugin='9d8a96b4-8145-416d-bd18-11402bc58f8d',
        table='grade_results'
    ),
    DataSetMetadata(
        plugin='533f84c8-b2ad-4688-94dc-c839952e9c4f',
        table='user_enrollments'
    )
]

DIFF_DATA_SET_METADATA = [
    DataSetMetadata(
        plugin='a78735f2-7210-4a57-aac1-e0f6bd714349',
        table='user_enrollments'
    )
]

def get_config():
    with open(CONFIG_LOCATION, 'r') as f:
        return json.load(f)

def trade_in_refresh_token(config):
    # https://tools.ietf.org/html/rfc6749#section-6
    response = requests.post(
        '{}/core/connect/token'.format(config['auth_service']),
        # Content-Type 'application/x-www-form-urlencoded'
        data={
            'grant_type': 'refresh_token',
            'refresh_token': config['refresh_token'],
            'scope': 'datahub:dataexports:*'
        },
        auth=HTTPBasicAuth(config['client_id'], config['client_secret'])
    )

    if response.status_code != 200:
        logger.error('Status code: %s; content: %s', response.status_code, response.text)
        response.raise_for_status()

    return response.json()

def put_config(config):
    with open(CONFIG_LOCATION, 'w') as f:
        json.dump(config, f, sort_keys=True)

def get_with_auth(endpoint, access_token):
    headers = {'Authorization': 'Bearer {}'.format(token_response['access_token'])}
    response = requests.get(endpoint, headers=headers)

    if response.status_code != 200:
        logger.error('Status code: %s; content: %s', response.status_code, response.text)
        response.raise_for_status()

    return response

def get_plugin_link_mapping(config, access_token):
    data_sets = []
    next_page_url = '{bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds'.format(
        bspace_url=config['bspace_url'],
        lp_version=API_VERSION
    )

    while next_page_url is not None:
        list_response = get_with_auth(next_page_url, access_token)
        list_json = list_response.json()

        data_sets += list_json['BrightspaceDataSets']
        next_page_url = list_json['NextPageUrl']

    return { d['PluginId']: d['DownloadLink'] for d in data_sets }

def get_number_of_columns(db_conn_params, table):
    with psycopg2.connect(**db_conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('''
                    SELECT *
                    FROM {table}
                    LIMIT 0;
                ''')
                .format(
                    table=sql.Identifier(table)
                )
            )

            return len(cur.description)

def process_csv_stream(csv_input_stream, num_columns_in_table):
    '''
    Ignore excessive columns in the CSV due to additive changes / BDS minor
    changes by ignoring any columns in the CSV past the number of columns in the
    table
    '''
    csv_rows = []
    csv_reader = csv.reader(csv_input_stream, quoting=csv.QUOTE_MINIMAL)
    for line in csv_reader:
        csv_rows.append(line[:num_columns_in_table])

    csv_data = io.StringIO()
    csv_writer = csv.writer(csv_data, quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerows(csv_rows)

    # Rewind the stream to the beginning before returning it
    csv_data.seek(io.SEEK_SET)
    return csv_data

def update_db(db_conn_params, table, csv_data):
    '''
    In a single transaction, update the table by:
    - Loading the CSV data into a temporary table
    - Run an update or insert query to update the main table with the data in
      the temporary table
    - Delete the temporary table
    '''
    with psycopg2.connect(**db_conn_params) as conn:
        with conn.cursor() as cur:
            tmp_table_id = sql.Identifier('tmp_' + table)

            cur.execute(
                sql.SQL('''
                    CREATE TEMP TABLE {tmp_table} AS
                        SELECT *
                        FROM {table}
                        LIMIT 0;
                ''')
                .format(
                    tmp_table=tmp_table_id,
                    table=sql.Identifier(table)
                )
            )

            cur.copy_expert(
                sql.SQL('''
                    COPY {tmp_table}
                    FROM STDIN
                    WITH (FORMAT CSV, HEADER);
                ''')
                .format(tmp_table=tmp_table_id),
                csv_data
            )

            upsert_query_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'schema',
                'upserts',
                'upsert_{table}.sql'.format(table=table)
            )
            with open(upsert_query_file) as upsert_query:
                cur.execute(upsert_query.read())

            cur.execute(sql.SQL('DROP TABLE {tmp_table}').format(tmp_table=tmp_table_id))

        conn.commit()

def batch_update_db(db_conn_params, table, csv_file, batch_size=10000):
    # Remove the first row, which contains the headers
    csv_file.readline()

    num_columns = get_number_of_columns(db_conn_params, table)
    csv_input_stream = io.StringIO()

    def update_db_with_batch(input_stream):
        '''
        Helper method that forms a closure so we don't have to pass many of the
        values used in this method as arguments
        '''

        # Rewind the stream to the beginning before passing it on
        input_stream.seek(io.SEEK_SET)

        with process_csv_stream(input_stream, num_columns) as csv_data:
            update_db(db_conn_params, table, csv_data)

        input_stream.close()

    i = 0
    for line in csv_file:
        csv_input_stream.write(line.decode('utf-8'))
        i += 1

        if i == batch_size:
            update_db_with_batch(csv_input_stream)
            csv_input_stream = io.StringIO()
            i = 0

    update_db_with_batch(csv_input_stream)

def unzip_and_update_db(response_content, db_conn_params, table):
    with io.BytesIO(response_content) as response_stream:
        with zipfile.ZipFile(response_stream) as zipped_data_set:
            files = zipped_data_set.namelist()

            assert len(files) == 1
            csv_name = files[0]

            with zipped_data_set.open(csv_name) as csv_file:
                batch_update_db(db_conn_params, table, csv_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script for downloading data sets.')
    parser.add_argument(
        '--differential',
        action='store_true',
        help='Use differential data sets instead of full data sets'
    )
    args = parser.parse_args()

    config = get_config()
    config['auth_service'] = config.get('auth_service', AUTH_SERVICE)

    token_response = trade_in_refresh_token(config)

    # Store the new refresh token for getting a new access token next run
    config['refresh_token'] = token_response['refresh_token']
    put_config(config)

    data_set_metadata = DIFF_DATA_SET_METADATA if args.differential else FULL_DATA_SET_METADATA
    plugin_to_link = get_plugin_link_mapping(config, token_response['access_token'])
    db_conn_params = {
        'host': config['dbhost'],
        'dbname': config['dbname'],
        'user': config['dbuser'],
        'password': config['dbpassword']
    }

    for plugin, table in data_set_metadata:
        response = get_with_auth(
            endpoint=plugin_to_link[plugin],
            access_token=token_response['access_token']
        )
        unzip_and_update_db(response.content, db_conn_params, table)
