# openfec_wrapper/utils.py
import os
import requests

FEC_API_KEY = os.environ.get('FEC_API_KEY', None)


class APIKeyMissingError(Exception):
    pass


if FEC_API_KEY is None:
    raise APIKeyMissingError(
        "All api requests require an API key. See "
        "https://api.data.gov/signup/"
        "to retrieve an authentication token from "
        "the Federal Election Commision, or email "
        "apiinfo@fec.gov for information about an "
        "upgraded api key."
    )

session = requests.Session()
session.params = {
    'per_page': '100',
    'sort_nulls_last': 'false',
    'sort': 'name',
    'page': '1',
    'api_key': FEC_API_KEY,
    'sort_null_only': 'false',
    'sort_hide_null': 'false',
}
session.headers = {
    'accept': 'application/json',
}
