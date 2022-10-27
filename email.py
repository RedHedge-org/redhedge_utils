"""
Utilities to read Emails on the bot accout.
"""

import email.parser as email_parser
import imaplib
from io import StringIO

import pandas as pd

try:
    from utils import get_env
except ImportError as exc:
    from .utils import get_env


def get_email_login():
    login = get_env("email-login")
    return login


def get_email_password():
    password = get_env("email-password")
    return password


def get_latest_email(subject):
    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    imap.login(get_email_login(), get_email_password())
    imap.select("INBOX")
    _, messages = imap.search(None, f'(SUBJECT "{subject}")')
    messages = messages[0].split()
    if len(messages) == 0:
        raise Exception(f"No email found with subject: {subject}")
    latest_email_id = messages[-1]
    _, data = imap.fetch(latest_email_id, "(RFC822)")
    imap.close()
    imap.logout()
    return data[0][1]


def get_data_frame_from_latest_email(subject):
    email = get_latest_email(subject)
    msg = email_parser.Parser().parsestr(email.decode("utf-8"))
    payload = None
    for part in msg.get_payload():
        payload = part.get_payload(decode=True)
        if len(payload) > 0:
            break
    df = pd.read_csv(StringIO(payload.decode("utf-8")))
    return df
