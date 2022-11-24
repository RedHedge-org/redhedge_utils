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
    received_date = msg["Received"]
    received_date = received_date.split(";")[1].strip()
    sent_date = msg["Date"]
    received_date = pd.to_datetime(received_date)
    received_date = received_date.tz_convert("UTC")
    sent_date = pd.to_datetime(sent_date)
    payload = None
    df = None
    for part in msg.get_payload():
        payload = part.get_payload(decode=True)
        try:
            df = pd.read_csv(StringIO(payload.decode("utf-8")))
        except Exception as exc:
            pass
        if df is not None:
            break
    df["received_date"] = received_date
    df["sent_date"] = sent_date
    return df
