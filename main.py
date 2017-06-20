import json
import requests
from requests.auth import HTTPBasicAuth

CONFIG_LOCATION = 'config.json'
AUTH_SERVICE = 'https://auth.brightspace.com/'

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

    return response.json()

def store_new_refresh_token(refresh_token):
    config = get_config()
    config['refresh_token'] = refresh_token
    with open(CONFIG_LOCATION, 'w') as f:
        f.write(json.dumps(config))

def make_request(endpoint, access_token):
    response = requests.get(
        endpoint,
        headers={'Authorization': 'Bearer {}'.format(access_token)}
    )

    print(response.json())

if __name__ == '__main__':
    config = get_config()
    config['auth_service'] = config.get('auth_service', AUTH_SERVICE)

    token_response = trade_in_refresh_token(config)
    store_new_refresh_token(token_response['refresh_token'])

    make_request(
        '{}/d2l/api/lp/1.9/users/whoami'.format(config['bspace_url']),
        token_response['access_token']
    )
