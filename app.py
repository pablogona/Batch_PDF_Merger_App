from flask import Flask, session, redirect, url_for, request, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv
import os
import json
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

# Load environment variables
load_dotenv()

app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)
app.secret_key = os.getenv('FLASK_SECRET_KEY')

# Google OAuth 2.0 setup
client_secrets_file = 'client_secret.json'

if not os.path.exists(client_secrets_file):
    raise FileNotFoundError("client_secret.json not found. Please download it from Google Cloud Console.")

# Import get_credentials and credentials_to_dict from backend.auth
from backend.auth import get_credentials, credentials_to_dict

# OAuth flow setup (remains the same)

@app.route('/')
def index():
    if 'credentials' not in session:
        return redirect(url_for('login'))
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/login')
def login():
    flow = Flow.from_client_secrets_file(
        client_secrets_file=client_secrets_file,
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ],
        redirect_uri=url_for('callback', _external=True)
    )
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true',
        prompt='consent'
    )
    session['state'] = state
    return redirect(authorization_url)

@app.route('/callback')
def callback():
    state = session.get('state')
    flow = Flow.from_client_secrets_file(
        client_secrets_file=client_secrets_file,
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ],
        state=state,
        redirect_uri=url_for('callback', _external=True)
    )
    flow.fetch_token(authorization_response=request.url)
    credentials = flow.credentials

    session['credentials'] = credentials_to_dict(credentials)
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

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

# Import and register the API blueprint
from backend.api_routes import api_bp
app.register_blueprint(api_bp, url_prefix='/api')

# Serve static files
@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

if __name__ == '__main__':
    app.run(debug=True)
