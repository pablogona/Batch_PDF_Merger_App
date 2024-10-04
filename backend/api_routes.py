# backend/api_routes.py

import multiprocessing
import uuid
import time
import json
from flask import Blueprint, request, jsonify, current_app
from google.oauth2.credentials import Credentials
from backend.drive_sheets import get_drive_service, get_sheets_service
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder
from backend.drive_sheets import get_drive_service, get_sheets_service, get_folder_ids
from backend.auth import get_credentials

# Initialize the Blueprint
api_bp = Blueprint('api_bp', __name__)

@api_bp.route('/process-pdfs', methods=['POST'])
def process_pdfs():
    """
    Endpoint to initiate PDF processing.
    """
    # Authenticate the user and retrieve credentials
    credentials = get_credentials()
    if not credentials:
        return jsonify({"status": "error", "message": "User not authenticated"}), 401

    # Initialize Google Drive and Sheets services
    try:
        drive_service = get_drive_service(credentials)
        sheets_service = get_sheets_service(credentials)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to initialize Google services: {str(e)}"}), 500

    # Create a unique folder name based on the current timestamp
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    folder_name = f"Proceso_{timestamp}"

    # Get or create the necessary folders in Google Drive
    try:
        main_folder_id, folder_ids = get_folder_ids(drive_service, folder_name)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to create folders: {str(e)}"}), 500

    # Retrieve uploaded files and form data
    excel_file = request.files.get('excelFile')
    sheets_file_id = request.form.get('sheetsFileId')
    folder_id = request.form.get('folderId')

    # Validate input data
    if not (excel_file or sheets_file_id) or not folder_id:
        return jsonify({"status": "error", "message": "Missing files or folder ID"}), 400

    # Read the uploaded Excel file if provided
    if excel_file:
        try:
            excel_file_content = excel_file.read()
            excel_filename = secure_filename(excel_file.filename)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Failed to read Excel file: {str(e)}"}), 400
    else:
        excel_file_content = None
        excel_filename = None

    # Generate a unique task ID for tracking
    task_id = f"task_{uuid.uuid4().hex}"

    # Access shared dictionaries from app config
    tasks_progress = current_app.config['tasks_progress']
    tasks_result = current_app.config['tasks_result']

    # Initialize progress and result for this task
    tasks_progress[task_id] = {'progress': 0, 'status': 'starting'}
    tasks_result[task_id] = {}

    # Start the PDF processing in a separate multiprocessing.Process
    try:
        process = multiprocessing.Process(
            target=process_task,
            args=(
                folder_id,
                excel_file_content,
                excel_filename,
                sheets_file_id,
                credentials.to_json(),
                folder_ids,
                main_folder_id,
                task_id,
                tasks_progress,
                tasks_result  # Pass shared dictionaries to the child process
            )
        )
        process.start()
    except Exception as e:
        # If process fails to start, update the task status
        tasks_progress[task_id] = {'progress': 100, 'status': 'error'}
        tasks_result[task_id] = {'status': 'error', 'message': f"Failed to start process: {str(e)}"}
        return jsonify({"status": "error", "message": f"Failed to start process: {str(e)}"}), 500

    # Return the task_id to the client for progress tracking
    return jsonify({"status": "success", "task_id": task_id}), 200


def process_task(folder_id, excel_file_content, excel_filename, sheets_file_id,
                 credentials_json, folder_ids, main_folder_id, task_id,
                 tasks_progress, tasks_result):
    """
    Target function for multiprocessing.Process.
    Handles PDF processing task in a separate process.
    """
    import json
    from google.oauth2.credentials import Credentials
    from backend.drive_sheets import get_drive_service, get_sheets_service

    try:
        # Access or initialize progress and result dictionaries
        progress_dict = tasks_progress.get(task_id, {'progress': 0, 'status': 'starting'})
        result_dict = tasks_result.get(task_id, {'status': 'in_progress'})

        # Reconstruct credentials from the provided JSON
        credentials_info = json.loads(credentials_json)
        credentials = Credentials(
            token=credentials_info['token'],
            refresh_token=credentials_info.get('refresh_token'),
            token_uri=credentials_info['token_uri'],
            client_id=credentials_info['client_id'],
            client_secret=credentials_info['client_secret'],
            scopes=credentials_info['scopes']
        )

        # Initialize Google Drive and Google Sheets services
        drive_service = get_drive_service(credentials)
        sheets_service = get_sheets_service(credentials)

        # Simulate some progress (e.g., 50% progress)
        progress_dict['progress'] = 50
        progress_dict['status'] = 'in_progress'
        tasks_progress[task_id] = progress_dict

        # Initialize the Manager and shared data structures
        manager = multiprocessing.Manager()
        pdf_info_list = manager.list()
        errors = manager.list()
        error_data = manager.list()
        error_files_set = manager.dict()

        # Perform PDF processing
        process_pdfs_in_folder(
            folder_id=folder_id,
            excel_file_content=excel_file_content,
            excel_filename=excel_filename,
            sheets_file_id=sheets_file_id,
            drive_service=drive_service,
            sheets_service=sheets_service,
            folder_ids=folder_ids,
            main_folder_id=main_folder_id,
            task_id=task_id,
            progress_dict=progress_dict,
            result_dict=result_dict,
            pdf_info_list=pdf_info_list,
            errors=errors,
            error_data=error_data,
            error_files_set=error_files_set
        )

        # Once processing is done, update progress and result status
        progress_dict['progress'] = 100
        progress_dict['status'] = 'completed'
        result_dict['status'] = 'success'
        result_dict['message'] = 'PDF processing completed successfully'

    except Exception as e:
        # In case of an error, update the task's progress and result with error details
        progress_dict['progress'] = 100
        progress_dict['status'] = 'error'
        result_dict['status'] = 'error'
        result_dict['message'] = str(e)

    finally:
        # Ensure final updates are made to the shared dictionaries
        tasks_progress[task_id] = progress_dict
        tasks_result[task_id] = result_dict


@api_bp.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    """
    Endpoint to retrieve the progress of a PDF processing task.
    """
    # Access shared dictionaries from app config
    tasks_progress = current_app.config['tasks_progress']
    tasks_result = current_app.config['tasks_result']

    # Retrieve the progress information for the given task_id
    progress = tasks_progress.get(task_id)
    result = tasks_result.get(task_id)

    if progress is None:
        return jsonify({'status': 'unknown task'}), 404

    # Prepare the response based on the current progress and result
    response = {
        'progress': progress.get('progress', 0),
        'status': progress.get('status', 'in_progress')
    }

    if progress.get('progress', 0) >= 100:
        if result:
            response['status'] = 'completed' if result.get('status') == 'success' else 'error'
            response['result'] = result
        else:
            response['status'] = 'completed'
            response['result'] = {'status': 'error', 'message': 'No result available.'}

    return jsonify(response), 200


@api_bp.route('/result/<task_id>', methods=['GET'])
def get_result(task_id):
    """
    Endpoint to retrieve the final result of a PDF processing task.
    """
    # Access shared dictionaries from app config
    tasks_progress = current_app.config['tasks_progress']
    tasks_result = current_app.config['tasks_result']

    # Retrieve the result for the given task_id
    result = tasks_result.get(task_id)

    if result is None:
        return jsonify({'status': 'unknown task'}), 404

    return jsonify(result), 200
