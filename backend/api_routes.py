import threading
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder
from backend.drive_sheets import (
    get_drive_service, get_sheets_service, upload_excel_to_drive,
    get_folder_ids
)
from backend.auth import get_credentials  # Import from backend.auth
import time

api_bp = Blueprint('api_bp', __name__)

# Define a global dictionary for progress tracking
progress_data = {}


@api_bp.route('/process-pdfs', methods=['POST'])
def process_pdfs():
    credentials = get_credentials()
    if not credentials:
        return jsonify({"status": "error", "message": "User not authenticated"}), 401

    drive_service = get_drive_service(credentials)
    sheets_service = get_sheets_service(credentials)

    timestamp = time.strftime('%Y%m%d_%H%M%S')  # Generate a timestamp
    folder_name = f"Proceso_{timestamp}"  # Create folder name using the timestamp

    main_folder_id, folder_ids = get_folder_ids(drive_service, folder_name)

    excel_file = request.files.get('excelFile')
    sheets_file_id = request.form.get('sheetsFileId')
    pdf_files = request.files.getlist('pdfFiles')

    if not (excel_file or sheets_file_id) or not pdf_files:
        return jsonify({"status": "error", "message": "Missing files"}), 400

    if excel_file:
        excel_file_id = upload_excel_to_drive(excel_file, drive_service, parent_folder_id=main_folder_id)
    else:
        excel_file_id = sheets_file_id

    # Start a thread to process PDFs without blocking the main thread
    task_id = f"task_{timestamp}"  # Unique task ID for tracking progress
    progress_data[task_id] = 0  # Initialize progress tracking
    thread = threading.Thread(target=process_task, args=(pdf_files, excel_file_id, drive_service, sheets_service, folder_ids, task_id))
    thread.start()

    return jsonify({"status": "success", "task_id": task_id})


def process_task(pdf_files, excel_file_id, drive_service, sheets_service, folder_ids, task_id):
    result = process_pdfs_in_folder(pdf_files, excel_file_id, drive_service, sheets_service, folder_ids, task_id)
    progress_data[task_id] = 100  # Mark progress as complete
    # You can add more handling of result if needed (like logging or saving output)


@api_bp.route('/progress/<task_id>', methods=['GET'])
def get_progress(task_id):
    progress = progress_data.get(task_id, 0)  # Default to 0% progress
    return jsonify({'progress': progress})

@api_bp.route('/task-result/<task_id>', methods=['GET'])
def get_task_result(task_id):
    # Check if task exists in progress_data
    if task_id in progress_data:
        if progress_data[task_id] == 100:  # Assuming 100 means completion
            return jsonify({"status": "success", "message": "Processing complete"})
        else:
            return jsonify({"status": "in_progress", "message": "Task is still in progress"}), 202
    else:
        return jsonify({"status": "error", "message": "Task not found"}), 404
