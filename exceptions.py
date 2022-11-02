"""
Utilities to handle exceptions
"""
import traceback

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
