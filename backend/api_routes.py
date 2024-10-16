# backend/api_routes.py

import logging
import multiprocessing
import uuid
import time
import json
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder, FileBasedStorage
from backend.drive_sheets import (
    upload_file_to_drive,
    update_google_sheet,
    get_or_create_folder,
    read_sheet_data,
    get_folder_ids,
    upload_excel_to_drive,
    batch_update_google_sheet,
    get_drive_service,
    get_sheets_service
)
from backend.auth import get_credentials

api_bp = Blueprint('api_bp', __name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    # Initialize FileBasedStorage within the endpoint
    file_storage = FileBasedStorage()

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

    try:
        # Initialize FileBasedStorage within the child process
        file_storage = FileBasedStorage()
        logger.info(f"File storage initialized in child process. Base path: {file_storage.base_path}")

        # Start processing PDFs
        process_pdfs_in_folder(
            folder_id, excel_file_content, excel_filename, sheets_file_id,
            drive_service, sheets_service, folder_ids, main_folder_id, task_id)
        
        # Mark overall progress as complete
        file_storage.set(f"progress:{task_id}", 100)
        logger.info(f"Task {task_id} completed. Progress set to 100%.")
    except Exception as e:
        logger.error(f"Error in processing task {task_id}: {e}", exc_info=True)
        # Handle exceptions and store error result
        file_storage.set(f"result:{task_id}", {'status': 'error', 'message': f'Ocurrió un error: {str(e)}'})
        # Ensure progress is marked as complete
        file_storage.set(f"progress:{task_id}", 100)

@api_bp.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    """
    Endpoint to retrieve the progress of a specific task.
    Returns:
        - JSON response with progress percentage and status.
    """
    # Initialize FileBasedStorage within the endpoint
    file_storage = FileBasedStorage()

    progress = file_storage.get(f"progress:{task_id}")
    logger.info(f"Retrieved progress for task {task_id}: {progress}")

    if progress is None:
        logger.warning(f"No progress found for task {task_id}")
        return jsonify({'status': 'unknown task'}), 404

    try:
        progress = float(progress)
    except ValueError:
        logger.error(f"Invalid progress value for task {task_id}: {progress}")
        return jsonify({'status': 'error', 'message': 'Invalid progress value.'}), 500

    response = {'progress': progress}

    if progress >= 100:
        result = file_storage.get(f"result:{task_id}")
        logger.info(f"Retrieved result for completed task {task_id}: {result}")
        
        if result:
            # Since result is already a dictionary, no need to call json.loads
            response['status'] = 'completed'
            response['result'] = result
        else:
            logger.warning(f"No result found for completed task {task_id}")
            response['status'] = 'completed'
            response['result'] = {'status': 'error', 'message': 'No result available.'}
    else:
        response['status'] = 'in_progress'

    logger.info(f"Returning response for task {task_id}: {response}")
    return jsonify(response), 200