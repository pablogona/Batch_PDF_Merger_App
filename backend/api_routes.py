from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.pdf_handler import process_pdfs_in_folder
from backend.drive_sheets import (
    get_drive_service, get_sheets_service, upload_excel_to_drive,
    upload_file_to_drive, get_or_create_folder, get_folder_ids
)
from backend.auth import get_credentials  # Import from backend.auth
import os
import io
import logging

api_bp = Blueprint('api_bp', __name__)

@api_bp.route('/process-pdfs', methods=['POST'])
def process_pdfs():
    credentials = get_credentials()
    if not credentials:
        return jsonify({"status": "error", "message": "User not authenticated"}), 401

    drive_service = get_drive_service(credentials)
    sheets_service = get_sheets_service(credentials)

    # Get folder IDs
    main_folder_id, folder_ids = get_folder_ids(drive_service)

    excel_file = request.files.get('excelFile')
    sheets_file_id = request.form.get('sheetsFileId')
    pdf_files = request.files.getlist('pdfFiles')

    if not (excel_file or sheets_file_id) or not pdf_files:
        return jsonify({"status": "error", "message": "Missing files"}), 400

    # Upload Excel file to Drive and get its ID
    if excel_file:
        excel_file_id = upload_excel_to_drive(excel_file, drive_service, parent_folder_id=main_folder_id)
    else:
        excel_file_id = sheets_file_id

    # Process PDFs and update Excel/Google Sheets
    result = process_pdfs_in_folder(
        pdf_files,
        excel_file_id,
        drive_service,
        sheets_service
    )

    if result['status'] == 'success':
        return jsonify({"status": "success"})
    else:
        return jsonify({"status": "error", "message": result.get('message', 'Error processing PDFs'), "errors": result.get('errors', [])}), 500
