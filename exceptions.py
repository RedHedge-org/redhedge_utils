"""
Utilities to handle exceptions
"""


def get_exception_info(exc: Exception) -> dict:
    """
    Get a dictionary containing information about an exception.

    This dictionary should be appended to the 'errors' field of responses.
    """
    return {
        "message": str(exc),
        "type": str(type(exc)),
        "lineno": exc.__traceback__.tb_lineno,
    }
