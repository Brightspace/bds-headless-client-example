import collections
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

API_VERSION = '1.15'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config.json'

DATA_SET_METADATA = [
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
            'scope': 'core:*:*'
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

def download_dataset(endpoint, access_token):
    headers = {'Authorization': 'Bearer {}'.format(token_response['access_token'])}
    response = requests.get(endpoint, headers=headers)

    if response.status_code != 200:
        logger.error('Status code: %s; content: %s', response.status_code, response.text)
        response.raise_for_status()

    return response

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

def unzip_and_update_db(response_content, db_conn_params, table):
    with io.BytesIO(response_content) as response_stream:
        with zipfile.ZipFile(response_stream) as zipped_data_set:
            files = zipped_data_set.namelist()

            assert len(files) == 1
            csv_name = files[0]

            with zipped_data_set.open(csv_name) as csv_data:
                update_db(db_conn_params, table, csv_data)

if __name__ == '__main__':
    config = get_config()
    config['auth_service'] = config.get('auth_service', AUTH_SERVICE)

    token_response = trade_in_refresh_token(config)

    # Store the new refresh token for getting a new access token next run
    config['refresh_token'] = token_response['refresh_token']
    put_config(config)

    db_conn_params = {
        'host': config['dbhost'],
        'dbname': config['dbname'],
        'user': config['dbuser'],
        'password': config['dbpassword']
    }

    for plugin, table in DATA_SET_METADATA:
        # Call {bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds/list for a list
        # of all available data sets
        endpoint = '{bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds/download/{plugin_id}'.format(
            bspace_url=config['bspace_url'],
            lp_version=API_VERSION,
            plugin_id=plugin
        )

        response = download_dataset(endpoint, token_response['access_token'])
        unzip_and_update_db(response.content, db_conn_params, table)
