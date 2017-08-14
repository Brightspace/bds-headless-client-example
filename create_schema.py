import json
import logging
import pathlib
import os

import psycopg2

logger = logging.getLogger(__name__)

CONFIG_LOCATION = 'config.json'

# Note: make sure the files in ./schema/tables are trusted!
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
SCHEMA_LOCATION = os.path.join(SCRIPT_PATH, 'schema', 'tables')

def get_db_conn_params():
    with open(CONFIG_LOCATION, 'r') as f:
        config = json.load(f)
        return {
            'host': config['dbhost'],
            'dbname': config['dbname'],
            'user': config['dbuser'],
            'password': config['dbpassword']
        }

if __name__ == '__main__':
    db_conn_params = get_db_conn_params()
    schema_files = [f.name for f in os.scandir(SCHEMA_LOCATION) if f.is_file()]

    with psycopg2.connect(**db_conn_params) as conn:
        with conn.cursor() as cur:

            for f in schema_files:
                schema_path = os.path.join(SCHEMA_LOCATION, f)
                logger.debug('Running {schema}'.format(schema=schema_path))
                with open(schema_path) as schema_query:
                    cur.execute(schema_query.read())

        conn.commit()
