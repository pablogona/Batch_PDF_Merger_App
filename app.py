from flask import Flask, send_from_directory, redirect, request, session, url_for, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import os
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import warnings

# Load environment variables
load_dotenv()

# Flask app setup
app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)
app.secret_key = os.getenv('FLASK_SECRET_KEY')

# Enable OAuth insecure transport for local development (HTTP)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

client_secrets_file = os.path.join(os.path.dirname(__file__), 'client_secret.json')

# Index route that ensures the user is authenticated
@app.route('/')
def index():
    if 'credentials' not in session:
        return redirect(url_for('login'))
    return send_from_directory(app.static_folder, 'index.html')

# Login route for Google OAuth
@app.route('/login')
def login():
    flow = Flow.from_client_secrets_file(
        client_secrets_file,
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ],
        redirect_uri=url_for('callback', _external=True)
    )
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        prompt='consent'
    )
    # Store only the `state`, which is a simple string (JSON serializable)
    session['state'] = state
    return redirect(authorization_url)

# OAuth2 callback route
@app.route('/callback')
def callback():
    state = session.get('state')
    if not state:
        return redirect(url_for('login'))

    flow = Flow.from_client_secrets_file(
        client_secrets_file,
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ],
        state=state,
        redirect_uri=url_for('callback', _external=True)
    )

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Ignore warnings
            flow.fetch_token(authorization_response=request.url)
    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500

    credentials = flow.credentials
    session['credentials'] = credentials_to_dict(credentials)
    session.pop('state', None)
    return redirect(url_for('index'))

# Logout route to clear session
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

# Check if the user is authenticated (used for frontend)
@app.route('/api/check-auth')
def check_auth():
    if 'credentials' not in session:
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True})

# Function to get credentials from session or refresh them if expired
def get_credentials():
    if 'credentials' not in session:
        return None

    credentials_info = session['credentials']
    credentials = Credentials(
        token=credentials_info['token'],
        refresh_token=credentials_info.get('refresh_token'),
        token_uri=credentials_info['token_uri'],
        client_id=credentials_info['client_id'],
        client_secret=credentials_info['client_secret'],
        scopes=credentials_info['scopes']
    )

    # If the credentials are expired, refresh them
    if credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            session['credentials'] = credentials_to_dict(credentials)
        except Exception as e:
            session.clear()
            return None
    return credentials

# Helper function to convert credentials to dictionary
def credentials_to_dict(credentials):
    return {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }

# Import and register the API blueprint (assumes you have this in backend/api_routes.py)
from backend.api_routes import api_bp
app.register_blueprint(api_bp, url_prefix='/api')

# Serve static files
@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

if __name__ == '__main__':
    app.run(debug=True)
