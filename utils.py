import os
from io import StringIO

import pandas as pd
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


def get_dataframe_from_csv_string(csv_content: str, **kwargs) -> pd.DataFrame:
    """
    Convert a CSV string in a DataFrame.

    Takes the same kwargs accepted by ``pandas.read_csv``.
    """
    csv_content = StringIO(csv_content)
    return pd.read_csv(csv_content, **kwargs)
