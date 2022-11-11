"""
Utilities to handle exceptions
"""
import datetime
import json
import traceback
from pathlib import Path

import pymongo

_LIMIT_STACK = 5


def get_exception_info(exc: Exception) -> dict:
    """
    Get a dictionary containing information about an exception.

    This dictionary should be appended to the 'errors' field of responses.
    """
    return {
        "message": str(exc),
        "type": str(type(exc)),
        "stack": traceback.format_exception(exc, limit=_LIMIT_STACK),
    }


def serialize_and_log_response(db: pymongo.database.Database, name_step: str, response: dict) -> str:
    """Serialize the response, and log it to a Mongo collection"""
    response["name_step"] = name_step
    response["event_datetime"] = datetime.datetime.utcnow()
    response["any_errors"] = any(response["errors"])
    try:
        collection = db.logs
        collection.update_one(
            {"name_step": name_step},
            {"$set": response},
            upsert=True,
        )
    except Exception as exc:
        pass
    finally:
        return json.dumps(response, default=str)
