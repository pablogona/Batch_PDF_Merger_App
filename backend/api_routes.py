# backend/api_routes.py

import threading
import io
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder, fetch_pdfs_from_drive_folder  # Import fetch function
from backend.drive_sheets import (
    get_drive_service, get_sheets_service, upload_excel_to_drive,
    get_folder_ids
)
from backend.auth import get_credentials
from backend.task_manager import progress_data, result_data  # Import shared data
import time

api_bp = Blueprint('api_bp', __name__)

@api_bp.route('/process-pdfs', methods=['POST'])
def process_pdfs():
    credentials = get_credentials()
    if not credentials:
        return jsonify({"status": "error", "message": "User not authenticated"}), 401

    drive_service = get_drive_service(credentials)
    sheets_service = get_sheets_service(credentials)

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    folder_name = f"Proceso_{timestamp}"

    main_folder_id, folder_ids = get_folder_ids(drive_service, folder_name)

    excel_file = request.files.get('excelFile')
    sheets_file_id = request.form.get('sheetsFileId')
    folder_id = request.form.get('folderId')  # Folder ID from Google Drive

    if not (excel_file or sheets_file_id) or not folder_id:
        return jsonify({"status": "error", "message": "Missing files or folder ID"}), 400

    # Read Excel file into memory if provided
    if excel_file:
        excel_file_content = excel_file.read()
        excel_filename = excel_file.filename
    else:
        excel_file_content = None
        excel_filename = None

    # Start a thread to process PDFs without blocking the main thread
    task_id = f"task_{timestamp}"
    progress_data[task_id] = 0  # Initialize progress tracking

    # Use the new function to fetch PDFs from Google Drive based on the folder ID
    thread = threading.Thread(target=process_task, args=(
        folder_id, excel_file_content, excel_filename, sheets_file_id,
        drive_service, sheets_service, folder_ids, main_folder_id, task_id))
    thread.start()

    return jsonify({"status": "success", "task_id": task_id})


def process_task(folder_id, excel_file_content, excel_filename, sheets_file_id,
                 drive_service, sheets_service, folder_ids, main_folder_id, task_id):
    # Fetch PDF files from the specified Google Drive folder
    pdf_files_data = fetch_pdfs_from_drive_folder(folder_id, drive_service)

    result = process_pdfs_in_folder(pdf_files_data, excel_file_content, excel_filename,
                                    sheets_file_id, drive_service, sheets_service,
                                    folder_ids, main_folder_id, task_id)
    progress_data[task_id] = 100  # Mark progress as complete
    result_data[task_id] = result  # Store the result


@api_bp.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    progress = progress_data.get(task_id, 0)
    response = {'progress': progress}

    if progress >= 100:
        response['status'] = 'completed'
        result = result_data.get(task_id, {})
        response['result'] = result
    else:
        response['status'] = 'in_progress'

    return jsonify(response)

