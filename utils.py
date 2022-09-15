import os

from dotenv import load_dotenv

load_dotenv()

_KEY_LOCAL_ENVIRONMENT = "local"


def get_mongo_uri():
    """get the mongo-uri from the open-faas secrets or from .env file"""
    if "MONGO_URI" in os.environ:
        return os.environ["MONGO_URI"]
    uri = ""
    with open("/var/openfaas/secrets/mongo-uri") as f:
        uri = f.read()
    return uri


def is_local():
    """check if the function is running locally."""
    return os.environ.get("ENVIRONMENT", None) == _KEY_LOCAL_ENVIRONMENT
