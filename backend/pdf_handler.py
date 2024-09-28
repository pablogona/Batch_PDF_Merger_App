# backend/pdf_handler.py

import io
import re
import logging
import multiprocessing
from functools import partial
import json
from pypdf import PdfReader, PdfWriter
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from backend.drive_sheets import (
    upload_file_to_drive,
    update_google_sheet,
    get_or_create_folder,
    read_sheet_data,
    get_folder_ids,
    upload_excel_to_drive,
    batch_update_google_sheet
)
from backend.utils import normalize_text
from backend.redis_client import redis_client  # Use Redis for progress tracking
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retry decorator for Google API calls to handle transient errors
@retry(
    retry=retry_if_exception_type(HttpError),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True
)
def list_drive_files(drive_service, folder_id, page_token):
    """
    List PDF files in a specific Google Drive folder.
    """
    response = drive_service.files().list(
        q=f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false",
        spaces='drive',
        fields='nextPageToken, files(id, name)',
        pageToken=page_token,
        pageSize=1000
    ).execute()
    return response

@retry(
    retry=retry_if_exception_type(HttpError),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True
)
def download_drive_file(drive_service, file_id):
    """
    Download the content of a PDF file from Google Drive.
    """
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return fh.getvalue()

def fetch_pdfs_from_drive_folder(folder_id, drive_service, task_id):
    """
    Fetch PDFs from a Google Drive folder by folder ID and return their content.
    Updates progress during the fetching process.
    """
    files = []
    page_token = None

    # Fetch all files to get the total count
    while True:
        try:
            response = list_drive_files(drive_service, folder_id, page_token)
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        except HttpError as e:
            logger.error(f"Error listing files in folder '{folder_id}': {e}")
            raise e  # Let the retry mechanism handle it

    total_pdfs = len(files)
    redis_client.set(f"progress:{task_id}:total", total_pdfs)

    if total_pdfs == 0:
        logger.warning(f"No PDFs found in folder {folder_id}.")
        return []

    pdf_files_data = []
    processed_pdfs = 0

    for file in files:
        try:
            # Download the PDF content
            file_content = download_drive_file(drive_service, file['id'])
            pdf_files_data.append({
                'filename': file['name'],
                'content': file_content
            })

            # Update progress (10% to 20%)
            processed_pdfs += 1
            progress_value = 10 + ((processed_pdfs / total_pdfs) * 10)  # Allocating 10% for fetching
            redis_client.set(f"progress:{task_id}", progress_value)
            logger.info(f"Fetched {processed_pdfs}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")
        except HttpError as e:
            logger.error(f"Failed to fetch PDF {file['name']}: {e}")
            continue  # Skip this file and continue with others

    return pdf_files_data

def process_pdfs_in_folder(folder_id, excel_file_content, excel_filename, sheets_file_id,
                           drive_service, sheets_service, folder_ids, main_folder_id, task_id):
    """
    Main function to process PDFs: fetch, extract information, pair, merge, and update sheets.
    """
    try:
        # Upload Excel file to Google Drive if provided
        if excel_file_content:
            excel_file_stream = io.BytesIO(excel_file_content)
            excel_file_stream.seek(0)
            excel_file_id = upload_excel_to_drive(
                excel_file_stream, excel_filename, drive_service, parent_folder_id=main_folder_id)
            logger.info(f"Excel file '{excel_filename}' uploaded as Google Sheet with ID: {excel_file_id}")
        elif sheets_file_id:
            excel_file_id = sheets_file_id
            logger.info(f"Using existing Google Sheet with ID: {excel_file_id}")
        else:
            raise ValueError("No Excel file content or Sheets file ID provided.")

        # Initialize progress to 10% after uploading Excel
        redis_client.set(f"progress:{task_id}", 10)

        # Fetch PDFs sequentially
        pdf_files_data = fetch_pdfs_from_drive_folder(folder_id, drive_service, task_id)
        logger.info(f"Total PDFs fetched for processing: {len(pdf_files_data)}")
        total_pdfs = len(pdf_files_data)
        redis_client.set(f"progress:{task_id}:total", total_pdfs)

        if total_pdfs == 0:
            logger.warning(f"No PDFs found in folder {folder_id}.")
            result = {
                'status': 'success',
                'message': 'No PDFs found to process.',
                'errors': []
            }
            redis_client.set(f"result:{task_id}", json.dumps(result))
            redis_client.set(f"progress:{task_id}", 100)
            return

        # Prepare for multiprocessing
        manager = multiprocessing.Manager()
        pdf_info_list = manager.list()
        errors = manager.list()

        # Initialize extraction progress
        redis_client.set(f"progress:{task_id}:completed_extraction", 0)

        # Create a partial function with fixed arguments
        extract_pdf_info_partial = partial(
            extract_pdf_info,
            pdf_info_list=pdf_info_list,
            errors=errors,
            drive_service=drive_service,
            folder_ids=folder_ids,
            task_id=task_id
        )

        # Process PDFs in parallel to extract info
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        pool.map(extract_pdf_info_partial, pdf_files_data)
        pool.close()
        pool.join()

        # Update progress to 60% after extraction
        redis_client.set(f"progress:{task_id}", 60)

        # Convert manager lists to regular lists
        pdf_info_list = list(pdf_info_list)
        errors = list(errors)

        # Pair PDFs based on names and types
        pairs, pairing_errors = pair_pdfs(pdf_info_list, folder_ids['PDFs con Error'], drive_service)
        errors.extend(pairing_errors)

        # Update progress after pairing
        redis_client.set(f"progress:{task_id}", 70)

        # Process pairs
        total_pairs = len(pairs)
        if total_pairs == 0:
            total_pairs = 1  # Prevent division by zero
        processed_pairs = 0

        # Initialize list to collect batch updates for Google Sheets
        batch_updates = []

        for pair in pairs:
            merged_pdf = merge_pdfs([pair['pdfs'][0], pair['pdfs'][1]])
            if merged_pdf:
                client_unique = update_google_sheet(
                    excel_file_id,
                    pair['name'],
                    pair['info'].get('folio_number'),
                    pair['info'].get('oficina'),
                    sheets_service,
                    batch_updates=batch_updates
                )

                if client_unique:
                    file_name = f"{client_unique} {pair['name']}.pdf"
                    upload_file_to_drive(merged_pdf, folder_ids['PDFs Unificados'], drive_service, file_name)
                    logger.info(f"Merged PDF for {pair['name']} uploaded to 'PDFs Unificados'")
                else:
                    errors.append({
                        'file_name': pair['file_name'],
                        'message': f"Client '{pair['name']}' not found in sheet."
                    })
                    logger.warning(f"Client '{pair['name']}' not found in sheet.")
            else:
                errors.append({
                    'file_name': pair['file_name'],
                    'message': f"Failed to merge PDFs for {pair['name']}"
                })
                logger.warning(f"Failed to merge PDFs for {pair['name']}")

            processed_pairs += 1
            progress_value = 70 + ((processed_pairs / total_pairs) * 29)  # Scale to 70-99%
            redis_client.set(f"progress:{task_id}", progress_value)
            logger.info(f"Processed pair {processed_pairs}/{total_pairs}. Progress: {progress_value:.1f}%")

        # After processing all pairs, perform batch update to Google Sheets
        if batch_updates:
            batch_update_google_sheet(excel_file_id, batch_updates, sheets_service)
            logger.info(f"Batch update to Google Sheets completed with {len(batch_updates)} updates.")
        else:
            logger.info("No updates to perform on Google Sheets.")

        # Prepare the final result
        result = {
            'status': 'success',
            'message': f'Processed {len(pairs)} pairs with {len(errors)} errors.',
            'errors': errors
        }

        # Store the result in Redis before setting progress to 100%
        redis_client.set(f"result:{task_id}", json.dumps(result))

        # Update overall progress to 100%
        redis_client.set(f"progress:{task_id}", 100)

    except Exception as e:
        # Handle exceptions and update Redis
        error_result = {'status': 'error', 'message': str(e)}
        redis_client.set(f"result:{task_id}", json.dumps(error_result))
        # Ensure overall progress is marked as complete
        redis_client.set(f"progress:{task_id}", 100)
        logger.error(f"Error processing PDFs: {str(e)}")

def extract_pdf_info(pdf_data, pdf_info_list, errors, drive_service, folder_ids, task_id):
    """
    Extract information from a single PDF and update shared lists.
    """
    pdf_filename = pdf_data['filename']
    pdf_content = pdf_data['content']
    pdf_stream = io.BytesIO(pdf_content)

    try:
        logger.info(f"Processing PDF: {pdf_filename}")

        # Upload original PDF to "PDFs Originales"
        try:
            upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs Originales'], drive_service, pdf_filename)
            logger.info(f"Successfully uploaded {pdf_filename} to 'PDFs Originales'")
        except Exception as e:
            logger.error(f"Error uploading original PDF '{pdf_filename}' to 'PDFs Originales': {e}")

        # Attempt to extract "ACUSE" info first
        info = extract_acuse_information(io.BytesIO(pdf_content))
        if not info:
            logger.info(f"Could not extract ACUSE info from {pdf_filename}. Trying DEMANDA extraction.")
            pdf_stream.seek(0)
            info = extract_demanda_information(io.BytesIO(pdf_content))
            if not info:
                logger.warning(f"Could not extract information from {pdf_filename}.")
                # Collect error information
                errors.append({
                    'file_name': pdf_filename,
                    'message': f"Could not extract information from PDF: {pdf_filename}"
                })
                # Optionally, upload the PDF to 'PDFs con Error'
                try:
                    upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
                    logger.info(f"Uploaded unmatched PDF '{pdf_filename}' to 'PDFs con Error'")
                except Exception as e:
                    logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")
            else:
                # Store extracted info
                pdf_info_list.append({
                    'file_name': pdf_filename,
                    'content': pdf_content,
                    'info': info
                })
        else:
            # Store extracted info
            pdf_info_list.append({
                'file_name': pdf_filename,
                'content': pdf_content,
                'info': info
            })

        # Update progress after processing each PDF
        completed = redis_client.incr(f"progress:{task_id}:completed_extraction")
        total_pdfs = int(redis_client.get(f"progress:{task_id}:total") or 1)
        progress_value = 10 + ((completed / total_pdfs) * 50)  # Scale to 10-60%
        redis_client.set(f"progress:{task_id}", progress_value)
        logger.info(f"Extracted info from {completed}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_filename}: {e}")
        # Collect error information
        errors.append({
            'file_name': pdf_filename,
            'message': str(e)
        })
        # Optionally, upload the PDF to 'PDFs con Error'
        try:
            upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
            logger.info(f"Uploaded error PDF '{pdf_filename}' to 'PDFs con Error'")
        except Exception as e:
            logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")

def pair_pdfs(pdf_info_list, error_folder_id, drive_service):
    """
    Pairs ACUSE and DEMANDA PDFs based on the extracted names and uploads unmatched PDFs to 'PDFs con Error'.
    """
    acuse_dict = {}
    demanda_dict = {}
    pairs = []
    errors = []

    # Separate PDFs into ACUSE and DEMANDA
    for pdf_info in pdf_info_list:
        pdf_type = pdf_info['info'].get('type')  # Corrected key
        name = pdf_info['info'].get('name')
        if pdf_type == 'ACUSE':
            acuse_dict[name] = pdf_info
        elif pdf_type == 'DEMANDA':
            demanda_dict[name] = pdf_info
        else:
            errors.append({
                'file_name': pdf_info['file_name'],
                'message': f"Unknown PDF type for {pdf_info['file_name']}"
            })
            logger.warning(f"Unknown PDF type for {pdf_info['file_name']}")

    # Pair PDFs based on the name
    for name in demanda_dict.keys():
        if name in acuse_dict:
            # Merge info from both DEMANDA and ACUSE
            combined_info = {**demanda_dict[name]['info'], **acuse_dict[name]['info']}
            pairs.append({
                'name': name,
                'pdfs': [demanda_dict[name]['content'], acuse_dict[name]['content']],
                'info': combined_info,  # Merged info
                'file_name': demanda_dict[name]['file_name']
            })
        else:
            # DEMANDA without matching ACUSE
            errors.append({
                'file_name': demanda_dict[name]['file_name'],
                'message': f"No matching ACUSE found for DEMANDA: {name}"
            })
            logger.warning(f"No matching ACUSE found for DEMANDA: {name}")
            # Optionally, upload DEMANDA to 'PDFs con Error'
            try:
                upload_file_to_drive(io.BytesIO(demanda_dict[name]['content']), error_folder_id, drive_service, demanda_dict[name]['file_name'])
                logger.info(f"Uploaded unmatched DEMANDA '{demanda_dict[name]['file_name']}' to 'PDFs con Error'")
            except Exception as e:
                logger.error(f"Error uploading DEMANDA '{demanda_dict[name]['file_name']}' to 'PDFs con Error': {e}")

    for name in acuse_dict.keys():
        if name not in demanda_dict:
            # ACUSE without matching DEMANDA
            errors.append({
                'file_name': acuse_dict[name]['file_name'],
                'message': f"No matching DEMANDA found for ACUSE: {name}"
            })
            logger.warning(f"No matching DEMANDA found for ACUSE: {name}")
            # Optionally, upload ACUSE to 'PDFs con Error'
            try:
                upload_file_to_drive(io.BytesIO(acuse_dict[name]['content']), error_folder_id, drive_service, acuse_dict[name]['file_name'])
                logger.info(f"Uploaded unmatched ACUSE '{acuse_dict[name]['file_name']}' to 'PDFs con Error'")
            except Exception as e:
                logger.error(f"Error uploading ACUSE '{acuse_dict[name]['file_name']}' to 'PDFs con Error': {e}")

    return pairs, errors

def merge_pdfs(pdfs):
    """
    Merge two PDFs (ACUSE and DEMANDA).
    """
    writer = PdfWriter()
    for pdf_content in pdfs:
        reader = PdfReader(io.BytesIO(pdf_content))
        for page in reader.pages:
            writer.add_page(page)

    merged_pdf = io.BytesIO()
    writer.write(merged_pdf)
    merged_pdf.seek(0)
    return merged_pdf

def extract_demanda_information(pdf_stream):
    """
    Extract information from DEMANDA PDFs.
    """
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            extracted_text = page.extract_text()
            if extracted_text:
                text += extracted_text

        # Post-process the text for better extraction
        text = post_process_text(text)

        # Extract Name
        nombre_match = re.search(
            r'VS\s+([A-ZÁÉÍÓÚÑÜ\.\s]+?)(?=\s+(C\.\s+JUEZ|QUEJOSO|TERCERO|PRUEBAS|JUICIO|AMPARO|\n|$))',
            text, re.UNICODE
        )

        # Log the extracted field
        extracted_name = nombre_match.group(1).strip() if nombre_match else None
        logger.info(f"Extracted - Nombre (DEMANDA): {extracted_name}")

        if nombre_match and extracted_name:
            return {
                'name': extracted_name,
                'type': 'DEMANDA'  # Ensure the key is 'type'
            }
        else:
            logger.warning(f"Could not extract name from DEMANDA PDF.")
            return None
    except Exception as e:
        logger.error(f"Error during extraction (DEMANDA): {e}")
        return None

def extract_acuse_information(pdf_stream):
    """
    Extract information from ACUSE PDFs.
    """
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            extracted_text = page.extract_text()
            if extracted_text:
                text += extracted_text

        # Post-process the text for better extraction
        text = post_process_text(text)

        # Extract relevant information
        oficina_match = re.search(r'Oficina\s*de\s*Correspondencia\s*Común[\w\s,]*?(?=\s*Folio|Foliode|\s*$)', text)
        folio_match = re.search(r'Folio\s*de\s*registro:\s*(\d+/\d+)', text)
        nombre_match = re.search(r'BAZ\s*VS\s*(.*?)(?:\s*ANEXOS\.pdf|\s*\.pdf|\s*$)', text)

        # Log results
        extracted_oficina = oficina_match.group(0).strip() if oficina_match else None
        extracted_folio = folio_match.group(1).strip() if folio_match else None
        extracted_name = nombre_match.group(1).strip() if nombre_match else None
        logger.info(f"Extracted - Oficina: {extracted_oficina}, Folio: {extracted_folio}, Nombre: {extracted_name}")

        if extracted_oficina and extracted_folio and extracted_name:
            return {
                'oficina': extracted_oficina,
                'folio_number': extracted_folio,
                'name': extracted_name,
                'type': 'ACUSE'  # Ensure the key is 'type'
            }
        else:
            logger.warning(f"Could not extract all required fields from ACUSE PDF.")
            return None
    except Exception as e:
        logger.error(f"Error during extraction (ACUSE): {e}")
        return None

def post_process_text(text):
    """
    Apply corrections to text formatting.
    """
    # Replace known concatenated words with proper spacing
    text = text.replace("Oficinade", "Oficina de")
    text = text.replace("Foliode", "Folio de")
    # Add missing spaces between lowercase and uppercase letters
    text = add_missing_spaces(text)
    return text

def add_missing_spaces(text):
    """
    Add spaces where needed between words using regex.
    """
    return re.sub(r'([a-záéíóúñü])([A-ZÁÉÍÓÚÑÜ])', r'\1 \2', text)
