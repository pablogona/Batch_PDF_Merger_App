import io
import re
import logging
from pypdf import PdfReader, PdfWriter
from backend.drive_sheets import (
    upload_file_to_drive,
    update_google_sheet,
    get_or_create_folder,
    read_sheet_data
)
from backend.utils import normalize_text
import pandas as pd

def process_pdfs_in_folder(pdf_files, excel_file_id, drive_service, sheets_service):
    try:
        # Read data from the Excel/Google Sheets file
        sheet_data = read_sheet_data(excel_file_id, sheets_service)
        if sheet_data is None:
            return {'status': 'error', 'message': 'Failed to read the Excel/Google Sheets file.'}

        # Extract information from all PDFs
        pdf_info_list = []
        for pdf_file in pdf_files:
            pdf_content = pdf_file.read()
            info = extract_information(io.BytesIO(pdf_content))
            if info:
                pdf_info_list.append({
                    'file_name': pdf_file.filename,
                    'content': pdf_content,
                    'info': info
                })
            else:
                logging.warning(f"Could not extract info from {pdf_file.filename}")

        # Pair PDFs based on names and "acuse" presence
        pairs, errors = pair_pdfs(pdf_info_list)

        # Process pairs
        error_files = []
        for pair in pairs:
            merged_pdf = merge_pdfs(pair['pdfs'])
            if merged_pdf:
                # Save merged PDF to Drive
                folder_id = get_or_create_folder('PDF Merger App/Merged PDFs', drive_service)
                file_name = f"{pair['client_number']}_{pair['name']}.pdf"
                upload_file_to_drive(merged_pdf, folder_id, drive_service, file_name)

                # Update Google Sheets
                updated = update_google_sheet(
                    excel_file_id,
                    pair['client_number'],
                    pair['info']['folio_number'],
                    pair['info']['oficina'],
                    sheets_service
                )
                if not updated:
                    error_files.append({
                        'file_name': file_name,
                        'message': f"Client number {pair['client_number']} not found in sheet."
                    })
            else:
                error_files.append({
                    'file_name': pair['file_names'],
                    'message': f"Failed to merge PDFs for {pair['name']}"
                })

        # Handle errors
        if errors or error_files:
            error_folder_id = get_or_create_folder('PDF Merger App/PDFs con error', drive_service)
            for error_pdf in errors:
                # Upload problematic PDFs to error folder
                upload_file_to_drive(
                    io.BytesIO(error_pdf['content']),
                    error_folder_id,
                    drive_service,
                    error_pdf['file_name']
                )
            return {
                'status': 'error',
                'message': 'Some PDFs could not be processed.',
                'errors': error_files
            }
        else:
            return {'status': 'success'}

    except Exception as e:
        logging.error(f"Error processing PDFs: {str(e)}")
        return {'status': 'error', 'message': str(e)}

def extract_information(pdf_stream):
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        text = normalize_text(text)

        # Extract data using regular expressions
        oficina_match = re.search(r'oficina de correspondencia común[\w\s,]*', text)
        folio_match = re.search(r'folio de registro:\s*(\d+/\d+)', text)
        nombre_match = re.search(r'vs\s*(.*?)\s*(anexos|\.pdf)', text)

        if oficina_match and folio_match and nombre_match:
            return {
                'oficina': oficina_match.group(0).strip(),
                'folio_number': folio_match.group(1).strip(),
                'name': nombre_match.group(1).strip()
            }
        else:
            return None
    except Exception as e:
        logging.error(f"Error extracting information: {str(e)}")
        return None

def pair_pdfs(pdf_info_list):
    pairs = []
    errors = []
    name_to_pdfs = {}

    # Group PDFs by normalized name
    for pdf_info in pdf_info_list:
        name = normalize_text(pdf_info['info']['name'])
        pdf_info['normalized_name'] = name
        is_acuse = 'acuse' in pdf_info['file_name'].lower() or 'acuse' in pdf_info['info']['oficina']
        pdf_info['is_acuse'] = is_acuse

        if name not in name_to_pdfs:
            name_to_pdfs[name] = []
        name_to_pdfs[name].append(pdf_info)

    # Pair PDFs
    for name, pdfs in name_to_pdfs.items():
        acuse_pdfs = [p for p in pdfs if p['is_acuse']]
        other_pdfs = [p for p in pdfs if not p['is_acuse']]

        if len(acuse_pdfs) == 1 and len(other_pdfs) == 1:
            pairs.append({
                'name': name,
                'pdfs': [io.BytesIO(acuse_pdfs[0]['content']), io.BytesIO(other_pdfs[0]['content'])],
                'info': acuse_pdfs[0]['info'],  # Assume info is the same
                'client_number': name,  # Placeholder, will retrieve actual client number
                'file_names': [acuse_pdfs[0]['file_name'], other_pdfs[0]['file_name']]
            })
        else:
            errors.extend(pdfs)

    return pairs, errors

def merge_pdfs(pdf_streams):
    try:
        writer = PdfWriter()
        for pdf_stream in pdf_streams:
            reader = PdfReader(pdf_stream)
            for page in reader.pages:
                writer.add_page(page)
        merged_pdf_stream = io.BytesIO()
        writer.write(merged_pdf_stream)
        merged_pdf_stream.seek(0)
        return merged_pdf_stream
    except Exception as e:
        logging.error(f"Error merging PDFs: {str(e)}")
        return None
