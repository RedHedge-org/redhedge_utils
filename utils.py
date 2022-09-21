import json
import os
from io import StringIO

import pandas as pd
import pymongo
import requests
from dotenv import load_dotenv

load_dotenv()

_KEY_LOCAL_ENVIRONMENT = "local"


class UnconfiguredEnvironment(Exception):
    """
    Raised when a needed environment variable is not set
    """


def get_mongo_uri():
    """get the mongo-uri from the open-faas secrets or from .env file"""
    uri = os.environ.get("MONGO_URI", None)
    if uri is None:
        with open("/var/openfaas/secrets/mongo-uri") as f:
            uri = f.read()
    return uri


def get_pnl_db(use_test_db: bool = False) -> pymongo.database.Database:
    """Get the PNL database"""
    uri = get_mongo_uri()
    client = pymongo.MongoClient(uri)
    if not use_test_db:
        db = client.pnl
    else:
        db = client.pnl_test_local
    return db


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


def _get_blg_df_random(tickers: list = [], fields: list = []) -> pd.DataFrame:
    import numpy as np

    n_tickers = len(tickers)
    n_fields = len(fields)
    rng = np.random.default_rng()
    df = pd.DataFrame(
        data=rng.random((n_tickers, n_fields)),
        index=tickers,
        columns=fields,
    )


def _get_blg_df_from_api(
    url: str, tickers: list = [], fields: list = [], YAS_YIELD_FLAG=None
) -> pd.DataFrame:
    payload = json.dumps(
        {"tickers": tickers, "fields": fields, "YAS_YIELD_FLAG": YAS_YIELD_FLAG}
    )
    response = requests.post(url, data=payload)
    df = pd.DataFrame(response.json())


def bdp_wrapper(tickers=[], fields=[], YAS_YIELD_FLAG=None):
    """wrapper for the function to check if the function is running locally or not"""
    env_var = "bloomberg-api-url"
    url = get_env(env_var)
    if url is not None:
        df = _get_blg_df_from_api(url=url, tickers=tickers, fields=fields)
    else:
        if is_local():
            df = _get_blg_df_random(tickers=tickers, fields=fields)
        else:
            raise UnconfiguredEnvironment(
                f"`{env_var}` environment variable is not set"
            )
    return df


def get_dataframe_from_csv_string(csv_content: str, **kwargs) -> pd.DataFrame:
    """
    Convert a CSV string in a DataFrame.

    Takes the same kwargs accepted by ``pandas.read_csv``.
    """
    csv_content = StringIO(csv_content)
    return pd.read_csv(csv_content, **kwargs)
