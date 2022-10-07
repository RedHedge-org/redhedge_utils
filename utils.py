import json
import os
import re
from io import StringIO
from zoneinfo import ZoneInfo

import pandas as pd
import pymongo
import requests
from dotenv import load_dotenv

load_dotenv()

_KEY_LOCAL_ENVIRONMENT = "local"

_DEFAULT_REQUESTS_TIMEOUT = 10

DEFAULT_TIMEZONE = ZoneInfo("Europe/London")


class UnconfiguredEnvironment(Exception):
    """
    Raised when a needed environment variable is not set
    """


class UnableToConnect(Exception):
    """
    Raised for generic connection errors
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
    try:
        response = requests.post(url, data=payload, timeout=_DEFAULT_REQUESTS_TIMEOUT)
    except requests.exceptions.ConnectTimeout as exc:
        raise UnableToConnect("Connection timed out")
    df = pd.DataFrame(response.json())
    return df


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

def bdh_wrapper(tickers=[], fields=[], start_date=None, end_date=None):
    """wrapper for the function to check if the function is running locally or not"""
    env_var = "bloomberg-api-url"
    url = get_env(env_var) +"/timeseries"
    print("getting data from url: ", url)
    print("tickers: ", tickers)
    print("fields: ", fields)
    print("start_date: ", start_date)
    print("end_date: ", end_date)
    response = requests.post(url, json={"tickers": tickers, "fields": fields, "start_date": start_date, "end_date": end_date})
    # the response looks like this:
    # {
    #     "('BE6334364708 Corp', 'Last_Price')": {
    #         "1664755200000": 88.946,
    #         "1664841600000": 89.186,
    #         "1664928000000": 89.051,
    #         "1665014400000": 89.035,
    #         "1665100800000": 88.961
    #     },
    #     "('BE6328904428 Corp', 'Last_Price')": {
    #         "1664755200000": 73.27,
    #         "1664841600000": 74.0,
    #         "1664928000000": 73.577,
    #         "1665014400000": 73.453,
    #         "1665100800000": 73.165
    #     }
    # }
    # convert the response to be saved as a timeseries in mongo
    # example:
    #  {
    #   "date": ISODate("2020-01-03T05:00:00.000Z"),
    #   "isin": "BE6334364708"
    #   "i_spread": 0.24,
    #   "price": 88.946
    #  }
    df = pd.DataFrame()
    for key, value in response.json().items():
        isin, field = key.replace("(", "").replace(")", "").replace("'", "").split(",")
        df_temp = pd.DataFrame.from_dict(value, orient="index", columns=[field])
        df_temp.index = pd.to_datetime(df_temp.index, unit="ms")
        df_temp["isin"] = isin
        df = df.append(df_temp)
    df = df.reset_index().rename(columns={"index": "date"})
    return df
    
    




def get_dataframe_from_csv_string(csv_content: str, **kwargs) -> pd.DataFrame:
    """
    Convert a CSV string in a DataFrame.

    Takes the same kwargs accepted by ``pandas.read_csv``.
    """
    csv_content = StringIO(csv_content)
    return pd.read_csv(csv_content, **kwargs)


pattern_security_type_suffix = re.compile(
    r" (?P<security_type>Comdty|Corp|Govt|Equity|Curncy)"
)


def correlation_id_to_isin(correlation_id: str) -> str:
    return pattern_security_type_suffix.sub("", correlation_id)
