import io
import json
import logging
import zipfile

import psycopg2
import requests
from requests.auth import HTTPBasicAuth

API_VERSION = 'unstable'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config.json'
PLUGINS_AND_TABLE = [
    ('793668a8-2c58-4e5e-b263-412d28d5703f', 'grade_objects'),
    ('07a9e561-e22f-4e82-8dd6-7bfb14c91776', 'org_units'),
    ('1d6d722e-b572-456f-97c1-d526570daa6b', 'users'),
    ('9d8a96b4-8145-416d-bd18-11402bc58f8d', 'grade_results')
]

logger = logging.getLogger(__name__)

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
            'scope': 'datahub:*:*'
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

def get_zipped_data_set(config, plugin):
    endpoint = '{bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds/{plugin_id}'.format(
        bspace_url=config['bspace_url'],
        lp_version=API_VERSION,
        plugin_id=plugin
    )
    headers = {'Authorization': 'Bearer {}'.format(token_response['access_token'])}
    response = requests.get(endpoint, headers=headers)

    return zipfile.ZipFile(io.BytesIO(response.content))

def get_csv_data(zipped_data_set):
    files = zipped_data_set.namelist()
    assert len(files) == 1

    csv_name = files[0]
    # CSV file is UTF-8-BOM encoded
    csv_data = zipped_data_set.read(csv_name).decode('utf-8-sig')
    return csv_data

def update_db(db_conn_params, table, csv_data):
    with psycopg2.connect(**db_conn_params) as conn:
        with conn.cursor() as cur:
            '''
            Note: using '.format()' because the table name can not be a SQL
            parameter. This is safe in this context because 'table' is a
            hardcoded value. In other contexts, always use SQL parameters
            when possible.
            '''
            cur.execute('TRUNCATE TABLE {};'.format(table))
            cur.copy_expert(
                'COPY {} FROM STDIN WITH (FORMAT CSV, HEADER)'.format(table),
                io.StringIO(csv_data)
            )
            cur.execute('SELECT * FROM {};'.format(table))
            print(cur.fetchone())

        conn.commit()

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

    for plugin, table in PLUGINS_AND_TABLE:
        zipped_ds = get_zipped_data_set(config, plugin)
        csv_data = get_csv_data(zipped_ds)
        update_db(db_conn_params, table, csv_data)


