# bds-headless-client-example
Sample Brightspace Data Sets headless client using OAuth 2.0 refresh tokens

## Prerequisites

* [Brightspace Data
  Sets](https://community.brightspace.com/s/question/0D56100000xrq5eCAA/)
* [Registered OAuth 2.0
  application](http://docs.valence.desire2learn.com/basic/oauth2.html) and
  corresponding [refresh
  token](https://community.brightspace.com/s/article/ka1610000000pYqAAI/How-to-obtain-an-OAuth-2-0-Refresh-Token)
  with scope `datahub:*:*`
* [Python 3.6](https://www.python.org/)
* [PostgreSQL](https://www.postgresql.org/) server and database

## Setup

* Create the required tables by running the SQL scripts in
  [schema/tables](./schema/tables) on the database being used
* Dependent libraries installed by running `python -m pip install -r
  requirements.txt`
* A file named `config.json` based on the [sample file](config-sample.json)

### Configs

| key           | Value                                       |
| ------------- | ------------------------------------------- |
| bspace_url    | E.g. `https://myschool.brightspace.com`     |
| client_id     | From OAuth 2.0 application registration     |
| client_secret | From OAuth 2.0 application registration     |
| refresh_token | From `Prerequisites`                        |
| dbhost        | Hostname of the PostgreSQL server           |
| dbname        | Name of the database                        |
| dbuser        | Username for accessing the database         |
| dbpassword    | Password of the user accessing the database |

### Folder Structure

The following outlines the minimum number of files that should be present before
using this script.

```
.
+-- schema
|   +-- upserts
|       +-- ...
+-- config.json
+-- main.py
```

## Usage

```bash
python main.py
```
