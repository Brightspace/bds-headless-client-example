import collections
import datetime
import io
import json
import logging
import zipfile

import psycopg2
import requests
from requests.auth import HTTPBasicAuth

DataSetMetadata = collections.namedtuple('DataSetMetadata', ['plugin', 'table', 'update_query'])

logger = logging.getLogger(__name__)

API_VERSION = 'unstable'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config.json'

DATA_SET_METADATA = [
    DataSetMetadata(
        plugin='07a9e561-e22f-4e82-8dd6-7bfb14c91776',
        table='org_units',
        update_query='''
            INSERT INTO org_units
                SELECT
                    org_unit_id,
                    organization,
                    type,
                    name,
                    code,
                    start_date,
                    end_date,
                    is_active,
                    created_date
                FROM tmp_org_units
            ON CONFLICT ON CONSTRAINT org_units_pkey
            DO UPDATE SET
                organization = EXCLUDED.organization,
                type = EXCLUDED.type,
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                is_active = EXCLUDED.is_active,
                created_date = EXCLUDED.created_date
            ;
        '''
    ),
    DataSetMetadata(
        plugin='793668a8-2c58-4e5e-b263-412d28d5703f',
        table='grade_objects',
        update_query='''
            INSERT INTO grade_objects
                SELECT
                    grade_object_id,
                    org_unit_id,
                    parent_grade_object_id,
                    name,
                    type_name,
                    category_name,
                    start_date,
                    end_date,
                    is_auto_pointed,
                    is_formula,
                    is_bonus,
                    max_points,
                    can_exceed_max_grade,
                    exclude_from_final_grade_calc,
                    grade_scheme_id,
                    weight,
                    num_lowest_grades_to_drop,
                    num_highest_grades_to_drop,
                    weight_distribution_type,
                    created_date,
                    tool_name,
                    associated_tool_item_id,
                    last_modified
                FROM tmp_grade_objects
            ON CONFLICT ON CONSTRAINT grade_objects_pkey
            DO UPDATE SET
                org_unit_id = EXCLUDED.org_unit_id,
                parent_grade_object_id = EXCLUDED.parent_grade_object_id,
                name = EXCLUDED.name,
                type_name = EXCLUDED.type_name,
                category_name = EXCLUDED.category_name,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                is_auto_pointed = EXCLUDED.is_auto_pointed,
                is_formula = EXCLUDED.is_formula,
                is_bonus = EXCLUDED.is_bonus,
                max_points = EXCLUDED.max_points,
                can_exceed_max_grade = EXCLUDED.can_exceed_max_grade,
                exclude_from_final_grade_calc = EXCLUDED.exclude_from_final_grade_calc,
                grade_scheme_id = EXCLUDED.grade_scheme_id,
                weight = EXCLUDED.weight,
                num_lowest_grades_to_drop = EXCLUDED.num_lowest_grades_to_drop,
                num_highest_grades_to_drop = EXCLUDED.num_highest_grades_to_drop,
                weight_distribution_type = EXCLUDED.weight_distribution_type,
                created_date = EXCLUDED.created_date,
                tool_name = EXCLUDED.tool_name,
                associated_tool_item_id = EXCLUDED.associated_tool_item_id,
                last_modified = EXCLUDED.last_modified
            ;
        '''
    ),
    DataSetMetadata(
        plugin='1d6d722e-b572-456f-97c1-d526570daa6b',
        table='users',
        update_query='''
            INSERT INTO users
                SELECT
                    user_id,
                    user_name,
                    org_defined_id,
                    first_name,
                    middle_name,
                    last_name,
                    is_active,
                    Organization,
                    internal_email,
                    external_email,
                    signup_date
                FROM tmp_users
            ON CONFLICT ON CONSTRAINT users_pkey
            DO UPDATE SET
                user_name = EXCLUDED.user_name,
                org_defined_id = EXCLUDED.org_defined_id,
                first_name = EXCLUDED.first_name,
                middle_name = EXCLUDED.middle_name,
                last_name = EXCLUDED.last_name,
                is_active = EXCLUDED.is_active,
                organization = EXCLUDED.organization,
                internal_email = EXCLUDED.internal_email,
                external_email = EXCLUDED.external_email,
                signup_date = EXCLUDED.signup_date
            ;
        '''
    ),
    DataSetMetadata(
        plugin='9d8a96b4-8145-416d-bd18-11402bc58f8d',
        table='grade_results',
        update_query='''
            INSERT INTO grade_results
                SELECT
                    grade_object_id,
                    org_unit_id,
                    user_id,
                    points_numerator,
                    points_denominator,
                    weighted_numerator,
                    weighted_denominator,
                    is_released,
                    is_dropped,
                    last_modified,
                    last_modified_by,
                    comments,
                    private_comments
                FROM tmp_grade_results
            ON CONFLICT ON CONSTRAINT grade_results_pkey
            DO UPDATE SET
                points_numerator = EXCLUDED.points_numerator,
                points_denominator = EXCLUDED.points_denominator,
                weighted_numerator = EXCLUDED.weighted_numerator,
                weighted_denominator = EXCLUDED.weighted_denominator,
                is_released = EXCLUDED.is_released,
                is_dropped = EXCLUDED.is_dropped,
                last_modified = EXCLUDED.last_modified,
                last_modified_by = EXCLUDED.last_modified_by,
                comments = EXCLUDED.comments,
                private_comments = EXCLUDED.private_comments
            ;
        '''
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

def get_csv_data(config, plugin):
    endpoint = '{bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds/{plugin_id}'.format(
        bspace_url=config['bspace_url'],
        lp_version=API_VERSION,
        plugin_id=plugin
    )
    headers = {'Authorization': 'Bearer {}'.format(token_response['access_token'])}
    response = requests.get(endpoint, headers=headers)

    with io.BytesIO(response.content) as response_stream:
        with zipfile.ZipFile(response_stream) as zipped_data_set:
            files = zipped_data_set.namelist()

            assert len(files) == 1
            csv_name = files[0]

            # CSV file is UTF-8-BOM encoded
            csv_data = zipped_data_set.read(csv_name).decode('utf-8-sig')
            return csv_data

def update_db(db_conn_params, table, csv_data, update_query):
    with psycopg2.connect(**db_conn_params) as conn:
        with conn.cursor() as cur:
            '''
            Note: using '.format()' because the table name can not be a SQL
            parameter. This is safe in this context because 'table' is a
            hardcoded value. In other contexts, always use SQL parameters
            when possible.
            '''

            cur.execute(
                '''
                CREATE TEMP TABLE tmp_{table} AS
                    SELECT *
                    FROM {table}
                    LIMIT 0;
                '''
                .format(table=table)
            )

            with io.StringIO(csv_data) as csv_data_stream:
                cur.copy_expert(
                    '''
                    COPY tmp_{table}
                    FROM STDIN
                    WITH (FORMAT CSV, HEADER);
                    '''
                    .format(table=table),
                    csv_data_stream
                )

            cur.execute(update_query)
            cur.execute('DROP TABLE tmp_{table}'.format(table=table))

            cur.execute('SELECT * FROM {table} LIMIT 1;'.format(table=table))
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

    for plugin, table, update_query in DATA_SET_METADATA:
        csv_data = get_csv_data(config, plugin)
        update_db(db_conn_params, table, csv_data, update_query)


