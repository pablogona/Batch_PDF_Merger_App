# app.py

import multiprocessing
from flask import Flask, send_from_directory, redirect, request, session, url_for, jsonify, current_app
from flask_cors import CORS
from dotenv import load_dotenv
import os
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import warnings
from flask_session import Session
from werkzeug.middleware.proxy_fix import ProxyFix

# Load environment variables from .env file
load_dotenv()

# Function to convert Credentials to Dictionary
def credentials_to_dict(credentials):
    """
    Converts Google OAuth credentials to a dictionary for session storage.
    """
    return {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }

# Create the Flask app
app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)
app.secret_key = os.getenv('FLASK_SECRET_KEY')  # Ensure you have FLASK_SECRET_KEY set in your .env

# Configure server-side session storage
SESSION_FILE_DIR = '/tmp/flask_session'  # Use '/tmp' for session storage in environments with read-only file systems
os.makedirs(SESSION_FILE_DIR, exist_ok=True)  # Ensure the directory exists
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = SESSION_FILE_DIR
app.config['PREFERRED_URL_SCHEME'] = 'https'
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1)
Session(app)

# Enable OAuth insecure transport for local development (HTTP)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

# Path to the client_secret.json file for Google OAuth
client_secrets_file = os.path.join(os.path.dirname(__file__), 'client_secret.json')

# Function: Get Credentials - Retrieves and refreshes credentials from the session
def get_credentials():
    """
    Retrieves and refreshes Google OAuth2 credentials from the session.

    Returns:
        Credentials object or None if not authenticated.
    """
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

    # Refresh credentials if expired
    if credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            session['credentials'] = credentials_to_dict(credentials)
        except Exception as e:
            session.clear()
            return None
    return credentials

# ---------------------- Flask Routes ----------------------

# Index route that ensures the user is authenticated
@app.route('/')
def index():
    if 'credentials' not in session:
        return redirect(url_for('login'))
    return send_from_directory(app.static_folder, 'index.html')

# Route: Login - Initiates Google OAuth flow
@app.route('/login')
def login():
    """
    Initiates the Google OAuth2 flow to authenticate the user.
    """
    # Determine the redirect URI based on the environment
    if os.environ.get('TEST_MODE') == 'True':
        redirect_uri = os.environ.get('OAUTH_REDIRECT_URI')
    else:
        redirect_uri = os.environ.get('PRODUCTION_REDIRECT_URI')

    # Create the OAuth flow instance
    flow = Flow.from_client_secrets_file(
        client_secrets_file,
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ],
        redirect_uri=redirect_uri
    )

    # Generate the authorization URL and state
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        prompt='consent'
    )

    # Store the state in the session for security
    session['state'] = state
    return redirect(authorization_url)

# Route: OAuth2 Callback - Handles the response from Google OAuth
@app.route('/callback')
def callback():
    """
    Handles the OAuth2 callback from Google, exchanges the authorization code for tokens,
    and stores the credentials in the session.
    """
    state = session.get('state')
    if not state:
        return redirect(url_for('login'))

    # Recreate the OAuth flow with the state and redirect URI
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
            warnings.simplefilter("ignore")  # Suppress warnings
            flow.fetch_token(authorization_response=request.url)  # Exchange code for tokens
    except Exception as e:
        print(f"An error occurred during OAuth2 callback: {e}")
        return f"An error occurred: {e}", 500

    # Store the credentials in the session
    credentials = flow.credentials
    session['credentials'] = credentials_to_dict(credentials)
    session.pop('state', None)  # Remove the state from the session for security
    return redirect(url_for('index'))

# Route: Logout - Clears the user session
@app.route('/logout')
def logout():
    """
    Logs out the user by clearing the session.
    """
    session.clear()
    return redirect('/')

# Route: Check Authentication - Used by the frontend to verify if the user is authenticated
@app.route('/api/check-auth')
def check_auth():
    """
    Checks if the user is authenticated by verifying the presence of credentials in the session.
    """
    if 'credentials' not in session:
        return jsonify({"authenticated": False}), 401
    return jsonify({"authenticated": True})

# Route: Progress - Retrieves the progress of a specific task
@app.route('/api/progress/<task_id>', methods=['GET'])
def progress(task_id):
    """
    Retrieves the current progress and status of a specific PDF processing task.

    Parameters:
        task_id (str): The unique identifier of the task.

    Returns:
        JSON response containing progress percentage and status.
    """
    # Access shared dictionaries from app config
    tasks_progress = current_app.config['tasks_progress']
    tasks_result = current_app.config['tasks_result']

    progress = tasks_progress.get(task_id, None)
    if progress is None:
        return jsonify({'status': 'unknown task'}), 404

    response = {
        'progress': progress.get('progress', 0),
        'status': progress.get('status', 'in_progress')
    }

    if response['progress'] >= 100:
        result = tasks_result.get(task_id)
        if result:
            response['status'] = 'completed'
            response['result'] = result
        else:
            response['status'] = 'completed'
            response['result'] = {'status': 'error', 'message': 'No result available.'}

    return jsonify(response)

# Route: Process Result - Retrieves the final result of a specific task
@app.route('/api/process-result/<task_id>', methods=['GET'])
def process_result(task_id):
    """
    Retrieves the final result of a specific PDF processing task.

    Parameters:
        task_id (str): The unique identifier of the task.

    Returns:
        JSON response containing the result or a processing status.
    """
    # Access shared dictionaries from app config
    tasks_progress = current_app.config['tasks_progress']
    tasks_result = current_app.config['tasks_result']

    result = tasks_result.get(task_id)
    if result:
        return jsonify(result)
    else:
        return jsonify({'status': 'processing'}), 202  # Still processing

# Import and register the API blueprint from backend/api_routes.py
from backend.api_routes import api_bp
app.register_blueprint(api_bp, url_prefix='/api')

# Route: Serve Static Files - Handles all other routes by serving static files
@app.route('/<path:path>')
def serve_static(path):
    """
    Serves static files from the frontend directory.

    Parameters:
        path (str): The path to the static file.

    Returns:
        The requested static file.
    """
    return send_from_directory(app.static_folder, path)

# Entry Point: Run the Flask application
if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')  # Ensure 'spawn' is used on Windows

    # Start the manager for shared dictionaries
    manager = multiprocessing.Manager()
    tasks_progress = manager.dict()
    tasks_result = manager.dict()

    # Store shared dictionaries in app config
    app.config['tasks_progress'] = tasks_progress
    app.config['tasks_result'] = tasks_result

    # Run the Flask app
    app.run(host="0.0.0.0", port=8080, debug=False)
