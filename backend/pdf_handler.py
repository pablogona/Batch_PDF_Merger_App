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
from collections import defaultdict
import concurrent.futures
import pandas as pd  # Ensure pandas is imported
import unicodedata  # For text normalization

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

            # Update progress (10% to 30%)
            processed_pdfs += 1
            progress_value = 10 + ((processed_pdfs / total_pdfs) * 20)  # Allocating 20% for fetching
            redis_client.set(f"progress:{task_id}", progress_value)
            logger.info(f"Fetched {processed_pdfs}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")
        except HttpError as e:
            logger.error(f"Failed to fetch PDF {file['name']}: {e}")
            continue  # Skip this file and continue with others

    return pdf_files_data

def normalize_name(name):
    """
    Normalize names by converting to uppercase, removing accents, and stripping whitespace.
    """
    if not name:
        return ''

    # Replace common misread representations of 'Ñ' with 'N'
    name = name.replace('Ñ', 'N').replace('ñ', 'n').replace('N~', 'N').replace('n~', 'n')
    # Replace 'Ñ' with 'N'
    name = name.replace('Ñ', 'N').replace('ñ', 'n')
    # Remove accents
    name = unicodedata.normalize('NFD', name)
    name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
    # Convert to uppercase
    name = name.upper()
    # Replace multiple spaces with a single space
    name = re.sub(r'\s+', ' ', name)
    # Strip leading and trailing whitespace
    name = name.strip()
    return name

def process_pdfs_in_folder(folder_id, excel_file_content, excel_filename, sheets_file_id,
                           drive_service, sheets_service, folder_ids, main_folder_id, task_id):
    """
    Main function to process PDFs: fetch, extract information, pair, merge, and update sheets.
    Additionally, collects error data and creates an Excel file for PDFs with errors.
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

        # Fetch PDFs
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
        error_data = manager.list()  # Initialize error data collection
        error_files_set = manager.dict()  # To keep track of files added to error_data

        # Initialize extraction progress
        redis_client.set(f"progress:{task_id}:completed_extraction", 0)

        # Create a partial function with fixed arguments
        extract_pdf_info_partial = partial(
            extract_pdf_info,
            pdf_info_list=pdf_info_list,
            errors=errors,
            error_data=error_data,  # Pass error_data
            error_files_set=error_files_set,  # Pass error_files_set
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
        error_data = list(error_data)
        error_files_set = dict(error_files_set)

        # Pair PDFs based on names and types
        pairs, pairing_errors = pair_pdfs(pdf_info_list, folder_ids['PDFs con Error'], drive_service, error_data, error_files_set)
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

        # Create Excel file for PDFs with errors
        if error_data:
            df_errors = pd.DataFrame(error_data, columns=[
                'DOCUMENTO',
                'NOMBRE_CTE',
                'FOLIO DE REGISTRO',
                'OFICINA DE CORRESPONDENCIA',
                'ERROR'
            ])

            # Remove the 'CLIENTE_UNICO' column if it exists
            if 'CLIENTE_UNICO' in df_errors.columns:
                df_errors.drop(columns=['CLIENTE_UNICO'], inplace=True)

            # Remove duplicate entries based on 'DOCUMENTO'
            df_errors.drop_duplicates(subset=['DOCUMENTO'], inplace=True)

            # Save DataFrame to Excel file in memory
            excel_buffer = io.BytesIO()
            df_errors.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)

            # Upload the Excel file to the process folder
            excel_file_name = 'PDFs con Error.xlsx'
            try:
                upload_file_to_drive(
                    excel_buffer,
                    main_folder_id,  # Upload to the process-specific folder
                    drive_service,
                    excel_file_name,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                logger.info(f"Excel file '{excel_file_name}' uploaded to process folder with ID: {main_folder_id}")
            except Exception as e:
                logger.error(f"Error uploading Excel file '{excel_file_name}': {e}")
        else:
            logger.info("No error data to write to Excel.")

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

def extract_pdf_info(pdf_data, pdf_info_list, errors, error_data, error_files_set, drive_service, folder_ids, task_id):
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

        # Classify the PDF
        pdf_type = classify_pdf(pdf_content, pdf_filename)
        logger.debug(f"PDF {pdf_filename} classified as {pdf_type}")

        # Extract information based on classification
        if pdf_type == 'DEMANDA':
            info = extract_demanda_information(io.BytesIO(pdf_content))
        elif pdf_type == 'ACUSE':
            info = extract_acuse_information(io.BytesIO(pdf_content))
        else:
            # Unable to classify PDF
            logger.warning(f"Unable to classify PDF {pdf_filename}.")
            partial_info = {
                'DOCUMENTO': pdf_filename,
                'NOMBRE_CTE': '',
                'FOLIO DE REGISTRO': '',
                'OFICINA DE CORRESPONDENCIA': '',
                'ERROR': "No se pudo clasificar el PDF."
            }
            # Add to error_data if not already added
            if pdf_filename not in error_files_set:
                error_data.append(partial_info)
                error_files_set[pdf_filename] = True
                errors.append({
                    'file_name': pdf_filename,
                    'message': "No se pudo clasificar el PDF."
                })
            # Upload to 'PDFs con Error'
            try:
                upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
                logger.info(f"Uploaded error PDF '{pdf_filename}' to 'PDFs con Error'")
            except Exception as e:
                logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")
            return

        if info is None:
            # Extraction failed
            logger.warning(f"Unable to extract valid information from PDF {pdf_filename}.")
            partial_info = {
                'DOCUMENTO': pdf_filename,
                'NOMBRE_CTE': '',
                'FOLIO DE REGISTRO': '',
                'OFICINA DE CORRESPONDENCIA': '',
                'ERROR': "No se pudo extraer información válida del PDF."
            }
            # Add to error_data if not already added
            if pdf_filename not in error_files_set:
                error_data.append(partial_info)
                error_files_set[pdf_filename] = True
                errors.append({
                    'file_name': pdf_filename,
                    'message': "No se pudo extraer información válida del PDF."
                })
            # Upload to 'PDFs con Error'
            try:
                upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
                logger.info(f"Uploaded error PDF '{pdf_filename}' to 'PDFs con Error'")
            except Exception as e:
                logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")
            return

        # Log the info extracted before normalization
        logger.debug(f"Extracted info before normalization: {info}")

        # Normalize the extracted name
        info['name'] = normalize_name(info.get('name', ''))

        # Log the info after normalization
        logger.debug(f"Info after normalization: {info}")

        # Check for missing critical fields based on PDF type
        if pdf_type == 'ACUSE':
            critical_fields = ['name', 'folio_number', 'oficina']
        elif pdf_type == 'DEMANDA':
            critical_fields = ['name']
        else:
            critical_fields = ['name']

        missing_fields = [field for field in critical_fields if not info.get(field)]
        if missing_fields:
            logger.warning(f"Missing critical fields {missing_fields} in PDF {pdf_filename}. Collecting partial data.")
            partial_info = {
                'DOCUMENTO': pdf_filename,
                'NOMBRE_CTE': info.get('name', ''),
                'FOLIO DE REGISTRO': info.get('folio_number', ''),
                'OFICINA DE CORRESPONDENCIA': info.get('oficina', ''),
                'ERROR': f"Faltan campos críticos: {', '.join(missing_fields)}"
            }
            if pdf_filename not in error_files_set:
                error_data.append(partial_info)
                error_files_set[pdf_filename] = True
                errors.append({
                    'file_name': pdf_filename,
                    'message': f"Faltan campos críticos: {', '.join(missing_fields)}"
                })
            # Upload to 'PDFs con Error'
            try:
                upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
                logger.info(f"Uploaded PDF with incomplete info '{pdf_filename}' to 'PDFs con Error'")
            except Exception as e:
                logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")
        else:
            # All critical fields are present
            pdf_info_list.append({
                'file_name': pdf_filename,
                'content': pdf_content,
                'info': info
            })

        # Update progress after processing each PDF
        completed = redis_client.incr(f"progress:{task_id}:completed_extraction")
        total_pdfs = int(redis_client.get(f"progress:{task_id}:total") or 1)
        progress_value = 30 + ((completed / total_pdfs) * 30)  # Scale to 30-60%
        redis_client.set(f"progress:{task_id}", progress_value)
        logger.info(f"Extracted info from {completed}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_filename}: {e}")
        errors.append({
            'file_name': pdf_filename,
            'message': str(e)
        })
        partial_info = {
            'DOCUMENTO': pdf_filename,
            'NOMBRE_CTE': '',
            'FOLIO DE REGISTRO': '',
            'OFICINA DE CORRESPONDENCIA': '',
            'ERROR': str(e)
        }
        if pdf_filename not in error_files_set:
            error_data.append(partial_info)
            error_files_set[pdf_filename] = True
        # Upload to 'PDFs con Error'
        try:
            upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
            logger.info(f"Uploaded error PDF '{pdf_filename}' to 'PDFs con Error'")
        except Exception as e:
            logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")

def classify_pdf(pdf_content, filename):
    """
    Classify PDF as 'ACUSE' or 'DEMANDA' based on the presence of specific keywords in text or filename.
    """
    text = extract_text_from_pdf(io.BytesIO(pdf_content))
    text_lower = text.lower()
    filename_lower = filename.lower()
    
    logger.debug(f"Classifying PDF '{filename}'. Extracted text snippet: {text_lower[:200]}")

    if 'acuse' in text_lower or 'acuse' in filename_lower:
        logger.debug(f"Classified '{filename}' as ACUSE.")
        return 'ACUSE'
    elif 'medios preparatorios' in text_lower or 'escrito inicial' in text_lower or 'vs' in text_lower:
        logger.debug(f"Classified '{filename}' as DEMANDA.")
        return 'DEMANDA'
    else:
        logger.debug(f"Unable to classify '{filename}'.")
        return 'UNKNOWN'


def pair_pdfs(pdf_info_list, error_folder_id, drive_service, error_data, error_files_set):
    """
    Pairs ACUSE and DEMANDA PDFs based on the extracted names and uploads unmatched or duplicate PDFs to 'PDFs con Error'.
    Also collects error data for unmatched or duplicate PDFs.

    Args:
        pdf_info_list (list): List of dictionaries containing PDF information.
        error_folder_id (str): Google Drive folder ID for 'PDFs con Error'.
        drive_service: Google Drive service instance.
        error_data (list): List to append error entries for 'PDFs con Error.xlsx'.
        error_files_set (dict): Dictionary to track already processed error files.

    Returns:
        tuple: A tuple containing the list of valid pairs and a list of errors.
    """
    # Use defaultdict to handle multiple PDFs per name
    acuse_dict = defaultdict(list)
    demanda_dict = defaultdict(list)
    pairs = []
    errors = []

    # Separate PDFs into ACUSE and DEMANDA
    for pdf_info in pdf_info_list:
        pdf_type = pdf_info['info'].get('type')  # 'ACUSE' or 'DEMANDA'
        name = pdf_info['info'].get('name')
        if pdf_type == 'ACUSE' and name:
            acuse_dict[name].append(pdf_info)
        elif pdf_type == 'DEMANDA' and name:
            demanda_dict[name].append(pdf_info)
        else:
            # If type is missing but name is present, treat it as ACUSE
            if name:
                acuse_dict[name].append(pdf_info)
            else:
                if pdf_info['file_name'] not in error_files_set:
                    errors.append({
                        'file_name': pdf_info['file_name'],
                        'message': f"Tipo de PDF y nombre desconocidos o faltantes para {pdf_info['file_name']}"
                    })
                    error_files_set[pdf_info['file_name']] = True
                    logger.warning(f"Unknown or missing PDF type and name for {pdf_info['file_name']}")

    # Identify duplicate names
    duplicate_names_acuse = set()
    duplicate_names_demanda = set()

    # Check for duplicate ACUSEs
    for name, acuse_list in acuse_dict.items():
        if len(acuse_list) > 1:
            duplicate_names_acuse.add(name)
            for pdf_info in acuse_list:
                pdf_filename = pdf_info['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Se encontraron múltiples ACUSEs para el nombre: {name}"
                    })
                    error_files_set[pdf_filename] = True
                    logger.warning(f"Duplicate ACUSE found for name '{name}' in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': name,
                        'FOLIO DE REGISTRO': pdf_info['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': pdf_info['info'].get('oficina', ''),
                        'ERROR': f"Se encontraron múltiples ACUSEs para el nombre: {name}"
                    }
                    error_data.append(error_entry)

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(pdf_info['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded duplicate ACUSE '{pdf_filename}' to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading duplicate ACUSE '{pdf_filename}' to 'PDFs con Error': {e}")

    # Check for duplicate DEMANDAs
    for name, demanda_list in demanda_dict.items():
        if len(demanda_list) > 1:
            duplicate_names_demanda.add(name)
            for pdf_info in demanda_list:
                pdf_filename = pdf_info['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Se encontraron múltiples DEMANDAs para el nombre: {name}"
                    })
                    error_files_set[pdf_filename] = True
                    logger.warning(f"Duplicate DEMANDA found for name '{name}' in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': name,
                        'FOLIO DE REGISTRO': pdf_info['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': pdf_info['info'].get('oficina', ''),
                        'ERROR': f"Se encontraron múltiples DEMANDAs para el nombre: {name}"
                    }
                    error_data.append(error_entry)

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(pdf_info['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded duplicate DEMANDA '{pdf_filename}' to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading duplicate DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}")

    # Exclude duplicate names from pairing
    names_to_pair = set(acuse_dict.keys()) & set(demanda_dict.keys())
    names_to_pair -= (duplicate_names_acuse | duplicate_names_demanda)

    # Handle ACUSEs corresponding to duplicate DEMANDAs
    for name in duplicate_names_demanda:
        if name in acuse_dict:
            acuse_list = acuse_dict[name]
            for acuse_pdf in acuse_list:
                pdf_filename = acuse_pdf['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Se encontró una ACUSE para el nombre con múltiples DEMANDAs: {name}"
                    })
                    error_files_set[pdf_filename] = True
                    logger.warning(f"ACUSE for duplicated DEMANDA name '{name}' in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': name,
                        'FOLIO DE REGISTRO': acuse_pdf['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': acuse_pdf['info'].get('oficina', ''),
                        'ERROR': f"Se encontró una ACUSE para el nombre con múltiples DEMANDAs: {name}"
                    }
                    error_data.append(error_entry)

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(acuse_pdf['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded ACUSE '{pdf_filename}' for duplicated DEMANDA name to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading ACUSE '{pdf_filename}' to 'PDFs con Error': {e}")

    # Handle DEMANDAs corresponding to duplicate ACUSEs
    for name in duplicate_names_acuse:
        if name in demanda_dict:
            demanda_list = demanda_dict[name]
            for demanda_pdf in demanda_list:
                pdf_filename = demanda_pdf['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Se encontró una DEMANDA para el nombre con múltiples ACUSEs: {name}"
                    })
                    error_files_set[pdf_filename] = True
                    logger.warning(f"DEMANDA for duplicated ACUSE name '{name}' in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': name,
                        'FOLIO DE REGISTRO': demanda_pdf['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': demanda_pdf['info'].get('oficina', ''),
                        'ERROR': f"Se encontró una DEMANDA para el nombre con múltiples ACUSEs: {name}"
                    }
                    error_data.append(error_entry)

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(demanda_pdf['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded DEMANDA '{pdf_filename}' for duplicated ACUSE name to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}")

    # Pair PDFs based on the name
    for name in names_to_pair:
        acuse_list = acuse_dict[name]
        demanda_list = demanda_dict[name]

        if len(acuse_list) == 1 and len(demanda_list) == 1:
            acuse_pdf = acuse_list[0]
            demanda_pdf = demanda_list[0]

            # Merge info from both DEMANDA and ACUSE
            combined_info = {**demanda_pdf['info'], **acuse_pdf['info']}
            pairs.append({
                'name': name,
                'pdfs': [acuse_pdf['content'], demanda_pdf['content']],
                'info': combined_info,
                'file_name': demanda_pdf['file_name']  # Assuming DEMANDA is the primary file
            })
        else:
            # This should not happen as duplicates are already handled
            logger.error(f"Unexpected number of ACUSEs or DEMANDAs for name '{name}'")
            for pdf_info in acuse_list:
                pdf_filename = pdf_info['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Cantidad inesperada de ACUSEs para el nombre: {name}"
                    })
                    error_files_set[pdf_filename] = True
                    logger.warning(f"Unexpected number of ACUSEs for name '{name}' in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': name,
                        'FOLIO DE REGISTRO': pdf_info['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': pdf_info['info'].get('oficina', ''),
                        'ERROR': f"Cantidad inesperada de ACUSEs para el nombre: {name}"
                    }
                    error_data.append(error_entry)

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(pdf_info['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded unexpected ACUSE '{pdf_filename}' to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading unexpected ACUSE '{pdf_filename}' to 'PDFs con Error': {e}")

            for pdf_info in demanda_list:
                pdf_filename = pdf_info['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Cantidad inesperada de DEMANDAs para el nombre: {name}"
                    })
                    error_files_set[pdf_filename] = True
                    logger.warning(f"Unexpected number of DEMANDAs for name '{name}' in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': name,
                        'FOLIO DE REGISTRO': pdf_info['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': pdf_info['info'].get('oficina', ''),
                        'ERROR': f"Cantidad inesperada de DEMANDAs para el nombre: {name}"
                    }
                    error_data.append(error_entry)

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(pdf_info['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded unexpected DEMANDA '{pdf_filename}' to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading unexpected DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}")

    # Handle DEMANDAs without matching ACUSEs
    for name, demanda_list in demanda_dict.items():
        if name not in acuse_dict and name not in duplicate_names_acuse and name not in duplicate_names_demanda:
            for demanda_pdf in demanda_list:
                pdf_filename = demanda_pdf['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"No se encontró un ACUSE correspondiente para DEMANDA: {name}"
                    })
                    logger.warning(f"No matching ACUSE found for DEMANDA: {name} in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': demanda_pdf['info'].get('name', ''),
                        'FOLIO DE REGISTRO': '',
                        'OFICINA DE CORRESPONDENCIA': '',
                        'ERROR': f"No se encontró un ACUSE correspondiente para DEMANDA: {name}"
                    }
                    error_data.append(error_entry)
                    error_files_set[pdf_filename] = True

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(demanda_pdf['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded unmatched DEMANDA '{pdf_filename}' to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}")

    # Handle ACUSEs without matching DEMANDAs
    for name, acuse_list in acuse_dict.items():
        if name not in demanda_dict and name not in duplicate_names_acuse and name not in duplicate_names_demanda:
            for acuse_pdf in acuse_list:
                pdf_filename = acuse_pdf['file_name']
                if pdf_filename not in error_files_set:
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"No se encontró una DEMANDA correspondiente para ACUSE: {name}"
                    })
                    logger.warning(f"No matching DEMANDA found for ACUSE: {name} in file '{pdf_filename}'")

                    # Collect error data
                    error_entry = {
                        'DOCUMENTO': pdf_filename,
                        'NOMBRE_CTE': acuse_pdf['info'].get('name', ''),
                        'FOLIO DE REGISTRO': acuse_pdf['info'].get('folio_number', ''),
                        'OFICINA DE CORRESPONDENCIA': acuse_pdf['info'].get('oficina', ''),
                        'ERROR': f"No se encontró una DEMANDA correspondiente para ACUSE: {name}"
                    }
                    error_data.append(error_entry)
                    error_files_set[pdf_filename] = True

                    # Upload to 'PDFs con Error' folder
                    try:
                        upload_file_to_drive(
                            io.BytesIO(acuse_pdf['content']),
                            error_folder_id,
                            drive_service,
                            pdf_filename
                        )
                        logger.info(f"Uploaded unmatched ACUSE '{pdf_filename}' to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading ACUSE '{pdf_filename}' to 'PDFs con Error': {e}")

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

        # Remove ACUSE content if present
        text = remove_acuse_content(text)

        # Log the cleaned text for debugging
        logger.info(f"Cleaned DEMANDA Text:\n{text}")

        # Adjusted regex pattern to match the text structure
        nombre_match = re.search(
            r'VS\s*([A-ZÁÉÍÓÚÑÜ\s]+)\s*MEDIOS PREPARATORIOS',
            text,
            re.UNICODE | re.IGNORECASE
        )

        if not nombre_match:
            # Try alternative patterns if the first one doesn't match
            nombre_match = re.search(
                r'VS\s*([A-ZÁÉÍÓÚÑÜ\s]+)\s*ESCRITO INICIAL',
                text,
                re.UNICODE | re.IGNORECASE
            )

        if not nombre_match:
            logger.warning("No name match found in DEMANDA PDF.")
            return None

        # Extract the name
        extracted_name = nombre_match.group(1).strip()
        logger.info(f"Extracted - Nombre (DEMANDA): {extracted_name}")

        info = {
            'name': extracted_name,
            'type': 'DEMANDA'  # Ensure the key is 'type'
        }

        return info

    except Exception as e:
        logger.error(f"Error during extraction (DEMANDA): {e}")
        return None

def extract_acuse_information(pdf_stream):
    """
    Extract information from ACUSE PDFs.
    """
    try:
        # Reading PDF
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            extracted_text = page.extract_text()
            if extracted_text:
                text += extracted_text

        # Optional: Post-process the text (custom function if needed)
        text = post_process_text(text)

        # Log the extracted text for debugging
        logger.info(f"Extracted Text from ACUSE PDF:\n{text}")

        # Extract 'nombre' using adjusted regex to exclude 'ANEXOS'
        nombre_match = re.search(
            r'BAZ\s*VS\s*([\wÁÉÍÓÚÑÜáéíóúñü\s]+?)(?=\s*ANEXOS\.pdf|\s*ANEXOS\s|\.pdf|\s*$)',
            text,
            re.UNICODE | re.IGNORECASE
        )

        # Extract 'oficina' using refined regex to isolate the office name
        oficina_match = re.search(
            r'Oficina\s*de\s*Correspondencia\s*Común\s*:\s*([\w\s,]+?)(?=\s*Folio|Foliode|\s*$)', 
            text
        )

        # Extract 'folio' number
        folio_match = re.search(r'Folio\s*de\s*registro:\s*(\d+/\d+)', text)

        # Extracted values
        extracted_name = nombre_match.group(1).strip() if nombre_match else ''
        extracted_oficina = oficina_match.group(1).strip() if oficina_match else ''
        extracted_folio = folio_match.group(1).strip() if folio_match else ''

        # Log the extracted data
        logger.info(f"Extracted - Oficina: {extracted_oficina}, Folio: {extracted_folio}, Nombre: {extracted_name}")

        # If no name is found, return partial info with empty 'name' field
        if not extracted_name:
            logger.warning("No name match found in ACUSE PDF.")
            # Return the partial info
            return {
                'oficina': extracted_oficina,
                'folio_number': extracted_folio,
                'name': extracted_name,  # This will be empty
                'type': 'ACUSE'
            }

        # Return extracted data as a dictionary
        return {
            'oficina': extracted_oficina,
            'folio_number': extracted_folio,
            'name': extracted_name,
            'type': 'ACUSE'
        }

    except Exception as e:
        # Log any errors encountered
        logger.error(f"Error during extraction (ACUSE): {e}")
        return None

def post_process_text(text):
    """
    Apply corrections to text formatting.
    """
    # Replace known concatenated words with proper spacing
    text = text.replace("Oficinade", "Oficina de")
    text = text.replace("Foliode", "Folio de")
    # Normalize text to remove extra whitespace
    text = normalize_text(text)
    return text

def remove_acuse_content(text):
    """
    Removes ACUSE-related content from DEMANDA PDF text.
    """
    acuse_start = r'Acuse de envío de escrito'
    acuse_end = r'(PORTAL DE SERVICIOS EN LÍNEA DEL PODER JUDICIAL|RECIBIDO|EVIDENCIA CRIPTOGRÁFICA)'

    cleaned_text = re.sub(f'{acuse_start}.*?{acuse_end}', '', text, flags=re.DOTALL)
    return cleaned_text

def extract_text_from_pdf(pdf_stream):
    """
    Extract all text from a PDF stream.
    """
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            extracted_text = page.extract_text()
            if extracted_text:
                text += extracted_text
        return text
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {e}")
        return ''

def normalize_text(text):
    """
    Normalize text by removing extra whitespace and normalizing unicode characters.
    """
    # Remove extra whitespace
    text = ' '.join(text.split())
    # Normalize unicode characters
    text = unicodedata.normalize('NFKC', text)
    return text
