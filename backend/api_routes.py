# backend/api_routes.py

import threading
import io
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder
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
    pdf_files = request.files.getlist('pdfFiles')

    if not (excel_file or sheets_file_id) or not pdf_files:
        return jsonify({"status": "error", "message": "Missing files"}), 400

    # Read Excel file into memory if provided
    if excel_file:
        excel_file_content = excel_file.read()
        excel_filename = excel_file.filename
    else:
        excel_file_content = None
        excel_filename = None

    # Read PDF files into memory
    pdf_files_data = []
    for pdf_file in pdf_files:
        pdf_content = pdf_file.read()
        pdf_files_data.append({
            'filename': pdf_file.filename,
            'content': pdf_content
        })

    # Start a thread to process PDFs without blocking the main thread
    task_id = f"task_{timestamp}"
    progress_data[task_id] = 0  # Initialize progress tracking

    thread = threading.Thread(target=process_task, args=(
        pdf_files_data, excel_file_content, excel_filename, sheets_file_id,
        drive_service, sheets_service, folder_ids, main_folder_id, task_id))
    thread.start()

    return jsonify({"status": "success", "task_id": task_id})


def process_task(pdf_files_data, excel_file_content, excel_filename, sheets_file_id,
                 drive_service, sheets_service, folder_ids, main_folder_id, task_id):
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
