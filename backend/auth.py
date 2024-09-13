# backend/auth.py

from flask import session
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

def get_credentials():
    if 'credentials' not in session:
        return None

    credentials = Credentials(**session['credentials'])
    if credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            session['credentials'] = credentials_to_dict(credentials)
        except Exception as e:
            # Token refresh failed
            session.clear()
            return None
    return credentials

def credentials_to_dict(credentials):
    return {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
