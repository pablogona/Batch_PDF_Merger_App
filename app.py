# app.py

from flask import Flask, send_from_directory, redirect, request, session, url_for, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import json
import os
import logging
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import warnings
from flask_session import Session
from werkzeug.middleware.proxy_fix import ProxyFix

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Application starting")

# Load environment variables
load_dotenv()
logger.info("Environment variables loaded")

# Flask app setup
app = Flask(__name__, static_folder='frontend', static_url_path='')
CORS(app)

@app.before_request
def log_request_info():
    logger.info(f"Received request: {request.method} {request.url}")
    logger.info(f"Request headers: {request.headers}")
    if request.method == 'POST':
        logger.info(f"Request form data: {request.form}")
        logger.info(f"Request files: {request.files}")

app.secret_key = os.getenv('FLASK_SECRET_KEY')
logger.info("Flask app initialized")

# Configure server-side session storage
SESSION_FILE_DIR = '/tmp/flask_session'
os.makedirs(SESSION_FILE_DIR, exist_ok=True)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = SESSION_FILE_DIR
app.config['PREFERRED_URL_SCHEME'] = 'https'
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1)
Session(app)
logger.info("Session configured")

# Enable OAuth insecure transport for local development (HTTP)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

client_secrets_file = os.path.join(os.path.dirname(__file__), 'client_secret.json')
logger.info(f"Client secrets file path: {client_secrets_file}")

def get_credentials():
    logger.info("Attempting to get credentials")
    if 'credentials' not in session:
        logger.info("No credentials in session")
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

    if credentials.expired and credentials.refresh_token:
        logger.info("Credentials expired, attempting to refresh")
        try:
            credentials.refresh(Request())
            session['credentials'] = credentials_to_dict(credentials)
            logger.info("Credentials refreshed successfully")
        except Exception as e:
            logger.error(f"Error refreshing credentials: {e}")
            session.clear()
            return None
    logger.info("Credentials retrieved successfully")
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

@app.route('/')
def index():
    logger.info("Index route accessed")
    if 'credentials' not in session:
        logger.info("No credentials in session, redirecting to login")
        return redirect(url_for('login'))
    logger.info("Serving index.html")
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/login')
def login():
    logger.info("Login route accessed")
    if os.environ.get('TEST_MODE') == 'True':
        redirect_uri = os.environ.get('OAUTH_REDIRECT_URI')
    else:
        redirect_uri = os.environ.get('PRODUCTION_REDIRECT_URI')
    logger.info(f"Using redirect URI: {redirect_uri}")

    flow = Flow.from_client_secrets_file(
        client_secrets_file,
        scopes=[
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ],
        redirect_uri=redirect_uri
    )
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        prompt='consent'
    )
    session['state'] = state
    logger.info(f"Authorization URL generated: {authorization_url}")
    return redirect(authorization_url)

@app.route('/callback')
def callback():
    logger.info("Callback route accessed")
    state = session.get('state')
    if not state:
        logger.warning("No state in session, redirecting to login")
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
            warnings.simplefilter("ignore")
            flow.fetch_token(authorization_response=request.url)
        logger.info("OAuth token fetched successfully")
    except Exception as e:
        logger.error(f"An error occurred during OAuth callback: {e}")
        return f"An error occurred: {e}", 500

    credentials = flow.credentials
    session['credentials'] = credentials_to_dict(credentials)
    session.pop('state', None)
    logger.info("Credentials stored in session, redirecting to index")
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    logger.info("Logout route accessed")
    session.clear()
    logger.info("Session cleared, redirecting to index")
    return redirect('/')

@app.route('/api/check-auth')
def check_auth():
    logger.info("Check auth route accessed")
    if 'credentials' not in session:
        logger.info("No credentials in session")
        return jsonify({"authenticated": False}), 401
    logger.info("User authenticated")
    return jsonify({"authenticated": True})

@app.route('/api/process-result')
def process_result():
    logger.info("Process result route accessed")
    task_id = request.args.get('task_id')
    if not task_id:
        logger.warning("No task ID provided")
        return jsonify({"status": "error", "message": "Task ID not provided"}), 400

    from backend.pdf_handler import FileBasedStorage
    file_storage = FileBasedStorage()
    logger.info(f"Retrieving result for task ID: {task_id}")

    result = file_storage.get(f"result:{task_id}")
    if result:
        try:
            result = json.loads(result)
            logger.info(f"Result retrieved for task {task_id}")
        except json.JSONDecodeError:
            logger.error(f"Invalid result format for task {task_id}")
            result = {"status": "error", "message": "Invalid result format."}
    else:
        logger.info(f"No result found for task {task_id}, task still processing")
        result = {"status": "processing", "message": "The task is still processing."}

    folder_name = file_storage.get(f"folder_name:{task_id}")
    response = {"result": result}
    if folder_name:
        response["folder_name"] = folder_name
        logger.info(f"Folder name retrieved for task {task_id}: {folder_name}")

    logger.info(f"Returning response for task {task_id}")
    return jsonify(response)

# Import and register the API blueprint
from backend.api_routes import api_bp
app.register_blueprint(api_bp, url_prefix='/api')
logger.info("API blueprint registered")

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    logger.warning(f"Caught unhandled route: {path}")
    return jsonify({"status": "error", "message": "Route not found"}), 404

# At the end of app.py, after registering all routes and blueprints
logger.info("All routes and blueprints registered")

# Add these new log statements
logger.info("Preparing to start the Flask development server")
logger.info(f"__name__ is: {__name__}")

if __name__ == "__main__":
    logger.info("Inside __main__ block")
    logger.info("Starting Flask development server")
    app.run(host="0.0.0.0", port=8080, debug=True)
else:
    logger.info("Not in __main__, app might be imported")

# Add this at the very end of the file, outside any blocks
logger.info("End of app.py file reached")