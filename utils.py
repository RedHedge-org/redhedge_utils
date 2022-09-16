import json
import os
from urllib import response
import pandas as pd
import requests 
from dotenv import load_dotenv

load_dotenv()

_KEY_LOCAL_ENVIRONMENT = "local"


def get_mongo_uri():
    """get the mongo-uri from the open-faas secrets or from .env file"""
    uri = os.environ.get("MONGO_URI", None)
    if uri is None:
        with open("/var/openfaas/secrets/mongo-uri") as f:
            uri = f.read()
    return uri


def is_local():
    """check if the function is running locally."""
    return os.environ.get("ENVIRONMENT", None) == _KEY_LOCAL_ENVIRONMENT

def get_env(env_name):
    """get the environment variable from the open-faas secrets or from .env file"""
    env = os.environ.get(env_name, None)
    if env is None:
        with open(f"/var/openfaas/secrets/{env_name}") as f:
            env = f.read()
    return env

def bdp_wrapper(tickers = [], fields = [], YAS_YIELD_FLAG=None):
    """wrapper for the function to check if the function is running locally or not"""
    if is_local():
        return pd.DataFrame.empty
    url = get_env("bloomberg-api-url")
    if url is None:
        raise Exception("bloomberg-api-url is not set")
    payload = json.dumps({"tickers": tickers, "fields": fields, "YAS_YIELD_FLAG": YAS_YIELD_FLAG})
    response = requests.post(url, data=payload)
    return response.json()
    