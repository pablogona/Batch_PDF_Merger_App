# backend/api_routes.py

import multiprocessing
import uuid
import time
import json
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder
from backend.drive_sheets import (
    get_drive_service, get_sheets_service,
    get_folder_ids
)
from backend.auth import get_credentials
from backend.pdf_handler import FileBasedStorage  # Import FileBasedStorage

api_bp = Blueprint('api_bp', __name__)
file_storage = FileBasedStorage()  # Initialize FileBasedStorage

@api_bp.route('/process-pdfs', methods=['POST'])
def process_pdfs():
    """
    Endpoint to initiate PDF processing.
    Expects:
        - 'excelFile' (optional): Uploaded Excel file.
        - 'sheetsFileId' (optional): Google Sheets file ID.
        - 'folderId' (required): Google Drive folder ID containing PDFs.
    Returns:
        - JSON response with status and task_id.
    """
    credentials = get_credentials()
    if not credentials:
        return jsonify({"status": "error", "message": "Usuario no autenticado"}), 401

    drive_service = get_drive_service(credentials)
    sheets_service = get_sheets_service(credentials)

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    folder_name = f"Proceso_{timestamp}"

    main_folder_id, folder_ids = get_folder_ids(drive_service, folder_name)

    excel_file = request.files.get('excelFile')
    sheets_file_id = request.form.get('sheetsFileId')
    folder_id = request.form.get('folderId')  # Folder ID from Google Drive

    if not (excel_file or sheets_file_id) or not folder_id:
        return jsonify({"status": "error", "message": "Faltan archivos o ID de carpeta"}), 400

    # Read Excel file into memory if provided
    if excel_file:
        excel_file_content = excel_file.read()
        excel_filename = secure_filename(excel_file.filename)
    else:
        excel_file_content = None
        excel_filename = None

    # Generate a unique task ID
    task_id = f"task_{uuid.uuid4().hex}"

    # Initialize progress in FileBasedStorage
    file_storage.set(f"progress:{task_id}:total", 0)  # Total PDFs unknown at this point
    file_storage.set(f"progress:{task_id}:completed", 0)
    file_storage.set(f"progress:{task_id}", 0)  # Overall progress

    # Start a multiprocessing.Process to handle the task
    process = multiprocessing.Process(target=process_task, args=(
        folder_id, excel_file_content, excel_filename, sheets_file_id,
        credentials.to_json(), folder_ids, main_folder_id, task_id))
    process.start()

    return jsonify({"status": "success", "task_id": task_id}), 200

def process_task(folder_id, excel_file_content, excel_filename, sheets_file_id,
                 credentials_json, folder_ids, main_folder_id, task_id):
    """
    Target function for multiprocessing.Process.
    Processes PDFs and updates FileBasedStorage with progress and results.
    """
    # Recreate the Drive and Sheets services in the child process
    from google.oauth2.credentials import Credentials
    from backend.drive_sheets import get_drive_service, get_sheets_service

    credentials = Credentials.from_authorized_user_info(json.loads(credentials_json))
    drive_service = get_drive_service(credentials)
    sheets_service = get_sheets_service(credentials)

    from backend.pdf_handler import process_pdfs_in_folder

    try:
        # Start processing PDFs
        process_pdfs_in_folder(
            folder_id, excel_file_content, excel_filename, sheets_file_id,
            drive_service, sheets_service, folder_ids, main_folder_id, task_id)
        # Mark overall progress as complete
        file_storage.set(f"progress:{task_id}", 100)
    except Exception as e:
        # Handle exceptions and store error result
        file_storage.set(f"result:{task_id}", json.dumps({'status': 'error', 'message': f'Ocurrió un error: {str(e)}'}))
        # Ensure progress is marked as complete
        file_storage.set(f"progress:{task_id}", 100)

@api_bp.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    """
    Endpoint to retrieve the progress of a PDF processing task.
    Expects:
        - task_id: Unique identifier for the processing task.
    Returns:
        - JSON response with progress percentage and status.
    """
    progress = file_storage.get(f"progress:{task_id}")
    if progress is None:
        return jsonify({'status': 'unknown task'}), 404

    progress = float(progress)
    response = {'progress': progress}

    if progress >= 100:
        result = file_storage.get(f"result:{task_id}")
        if result:
            response['status'] = 'completed'
            response['result'] = json.loads(result)
        else:
            response['status'] = 'completed'
            response['result'] = {'status': 'error', 'message': 'No result available.'}
    else:
        response['status'] = 'in_progress'

    return jsonify(response)