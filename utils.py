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
        with open("/var/openfaas/secrets/k8s-mongo-uri") as f:
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
    url = get_env(env_var) + "/timeseries"
    print("getting data from url: ", url)
    print("tickers: ", tickers)
    print("fields: ", fields)
    print("start_date: ", start_date)
    print("end_date: ", end_date)
    response = requests.post(
        url,
        json={
            "tickers": tickers,
            "fields": fields,
            "start_date": start_date,
            "end_date": end_date,
        },
    )
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
        field = field.strip()
        df_temp = pd.DataFrame.from_dict(value, orient="index", columns=[field])
        df_temp.index = pd.to_datetime(df_temp.index, unit="ms")
        df_temp["isin"] = correlation_id_to_isin(isin)
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


PATTERN_CORRELATION_ID = re.compile(
    r"^(?P<isin>\w[\w\s]+\w)(@(?P<price_source>\w+))?\s+(?P<security_type>Comdty|Corp|Govt|Equity|Curncy|Index)$"
)

PATTERN_CDS_CORRELATION_ID = re.compile(r"^(?P<isin>\w+)_(?P<info>\w+)$")


def correlation_id_to_isin(correlation_id: str) -> str:
    if matched := PATTERN_CORRELATION_ID.match(correlation_id):
        return matched.group("isin")
    elif matched := PATTERN_CDS_CORRELATION_ID.match(correlation_id):
        return matched.group("isin")
    else:
        return correlation_id


_MAP_SECURITY_TYPE_BLOOMBERG_SUFFIX = {
    "Bond Corporate": "Corp",
    "Future": "Comdty",
    "Bond Sovereign": "Govt",
    "Credit Default Swap": "Curncy",
    "CDS Index Swap": "Curncy",
}


def create_correlation_id(
    isin: str,
    security_type: str,
    pricing_source: str = None,
    ignore_pricing_source: bool = False,
) -> str:
    """
    Create a Correlation ID from ISIN, security type, and pricing source.

    The pricing source is included in the Correlation ID only for
    securities of type 'Bond Corporate'.
    """
    try:
        bloomberg_suffix = _MAP_SECURITY_TYPE_BLOOMBERG_SUFFIX.get(security_type, None)
        if security_type == "Bond Corporate":
            if (pricing_source is not None) and (not ignore_pricing_source):
                correlation_id = f"{isin}@{pricing_source} {bloomberg_suffix}"
            else:
                correlation_id = f"{isin} {bloomberg_suffix}"
        else:
            if bloomberg_suffix is not None:
                correlation_id = f"{isin} {bloomberg_suffix}"
            else:
                correlation_id = isin
    except Exception as exc:
        raise ValueError(
            "Unable to create a Correlation ID for "
            f"{isin=}, {security_type=}, and {pricing_source=}"
        )
    else:
        return correlation_id


def get_correlation_id(row: pd.Series, ignore_pricing_source: bool = False) -> str:
    security_type = row["security_instrument_type_rh"]
    isin = row["isin"]
    pricing_source = row["security_default_pricing_source_rh"]
    correlation_id = create_correlation_id(
        isin=isin,
        security_type=security_type,
        pricing_source=pricing_source,
        ignore_pricing_source=ignore_pricing_source,
    )
    return correlation_id


_PATTERN_STRATEGY_CODE = re.compile(r"^(?P<book>[\w ]+?)(_(?P<portfolio>TRS\w+))?$")


def get_portfolio_from_strategy_code(strategy_code: str) -> str:
    matched = _PATTERN_STRATEGY_CODE.match(strategy_code)
    portfolio = matched.group("portfolio")
    if portfolio is None:
        return "MAIN"
    else:
        return portfolio


_PATTERN_WHITESPACES = re.compile(r"\s+")


def nullify_whitespaces(df: pd.DataFrame) -> None:
    df.replace({_PATTERN_WHITESPACES: None}, inplace=True)


if __name__ == "__main__":
    correlation_ids_with_expected_isins = {
        "ITXEX538  Curncy": "ITXEX538",
        "CBAR1E5 Curncy": "CBAR1E5",
        "GB00BDCHBW80 Govt": "GB00BDCHBW80",
        "FR0014006ZC4@BGN Corp": "FR0014006ZC4",
        "G Z2 Comdty": "G Z2",
        "GECU10Y Index": "GECU10Y",
        "ITRX XOVER CDSI GEN 5Y Corp": "ITRX XOVER CDSI GEN 5Y",
        "CY349216_271220": "CY349216",
    }
    for correlation_id, expected_isin in correlation_ids_with_expected_isins.items():
        found_isin = correlation_id_to_isin(correlation_id)
        assert expected_isin == found_isin

    correlation_ids_with_components = {
        "FR0014006ZC4@BGN Corp": ("FR0014006ZC4", "Bond Corporate", "BGN"),
        "GB00BDCHBW80 Govt": ("GB00BDCHBW80", "Bond Sovereign", "BGN"),
        "G Z2 Comdty": ("G Z2", "Future", "BGN"),
    }
    for (
        expected_correlation_id,
        (
            isin,
            security_type,
            pricing_source,
        ),
    ) in correlation_ids_with_components.items():
        found_correlation_id = create_correlation_id(
            isin=isin, security_type=security_type, pricing_source=pricing_source
        )
        assert expected_correlation_id == found_correlation_id

    strategy_codes_with_expected_portfolios = {
        "VOON": "MAIN",
        "SEMINARA": "MAIN",
        "SEMINARA_TRSB": "TRSB",
        "SEMINARA_TRS1": "TRS1",
        "SUB INC": "MAIN",
    }
    for (
        strategy_code,
        expected_portfolio,
    ) in strategy_codes_with_expected_portfolios.items():
        found_portfolio = get_portfolio_from_strategy_code(strategy_code)
        assert expected_portfolio == found_portfolio
    print("Done!")

TEAMS_WEBHOOK = get_env("teams-webhook")


def teams_message(msg="", type="info", title="", url=""):
    """
    Send a message to Microsoft Teams channel
    """
    if not msg:
        return
    if not title:
        title = "Message from Python"
    if type == "info":
        color = "0076D7"
    elif type == "error":
        color = "FF0000"
    elif type == "success":
        color = "008000"
    else:
        color = "000000"
    data = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "title": title,
        "text": msg,
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "Open Link",
                "targets": [{"os": "default", "uri": url}],
            }
        ],
    }
    headers = {"Content-Type": "application/json"}
    requests.post(TEAMS_WEBHOOK, data=json.dumps(data), headers=headers)
