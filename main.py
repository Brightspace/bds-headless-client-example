import logging
import json
import requests
from requests.auth import HTTPBasicAuth

API_VERSION = '1.9'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config.json'

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

if __name__ == '__main__':
    config = get_config()
    config['auth_service'] = config.get('auth_service', AUTH_SERVICE)

    token_response = trade_in_refresh_token(config)

    # Store the new refresh token for getting a new access token next run
    config['refresh_token'] = token_response['refresh_token']
    put_config(config)

    response = requests.get(
        '{}/d2l/api/lp/{}/users/whoami'.format(config['bspace_url'], API_VERSION),
        headers={'Authorization': 'Bearer {}'.format(token_response['access_token'])}
    )
    print(response.json())
