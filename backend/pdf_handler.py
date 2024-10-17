# backend/pdf_handler.py

import io
import re
import logging
import multiprocessing
from functools import partial
import json
import os
import tempfile
import time
from collections import defaultdict
import pandas as pd
import unicodedata
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
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from filelock import FileLock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileBasedStorage:
    def __init__(self, base_path=None):
        self.base_path = base_path or os.environ.get('STORAGE_PATH', '/tmp')
        os.makedirs(self.base_path, exist_ok=True)
        # Removed self.lock as it's not safe across processes

    def _sanitize_key(self, key):
        return key.replace(':', '_').replace('/', '_')

    def _get_file_path(self, key):
        sanitized_key = self._sanitize_key(key)
        return os.path.join(self.base_path, f"{sanitized_key}.json")

    def set(self, key, value):
        file_path = self._get_file_path(key)
        lock_path = f"{file_path}.lock"
        logger.info(f"Attempting to write to file: {file_path}")
        try:
            # Validate JSON serialization before writing
            try:
                json.dumps(value)
            except (TypeError, OverflowError) as e:
                logger.error(f"Data for key '{key}' is not JSON serializable: {e}", exc_info=True)
                return

            with FileLock(lock_path):
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(value, f, ensure_ascii=False, indent=2)
            logger.info(f"Successfully wrote to file: {file_path}")
            # Verify the file was actually written
            if os.path.exists(file_path):
                logger.info(f"File {file_path} exists after writing")
            else:
                logger.error(f"File {file_path} does not exist after writing attempt")
        except IOError as e:
            logger.error(f"IOError writing to file {file_path}: {e}", exc_info=True)
            raise e  # Re-raise to handle upstream if necessary
        except Exception as e:
            logger.error(f"Unexpected error writing to file {file_path}: {e}", exc_info=True)
            raise e  # Re-raise to handle upstream if necessary

    def get(self, key):
        file_path = self._get_file_path(key)
        lock_path = f"{file_path}.lock"
        logger.info(f"Attempting to read from file: {file_path}")
        try:
            with FileLock(lock_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    value = json.load(f)
                    logger.info(f"Successfully read value from {file_path}: {value}")
                    return value
        except FileNotFoundError:
            logger.warning(f"File not found: {file_path}")
            return None
        except IOError as e:
            logger.error(f"Error reading file {file_path}: {e}", exc_info=True)
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in file {file_path}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error reading file {file_path}: {e}", exc_info=True)
            return None

    def incr(self, key):
        file_path = self._get_file_path(key)
        lock_path = f"{file_path}.lock"
        try:
            with FileLock(lock_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        value = json.load(f)
                except FileNotFoundError:
                    value = 0
                except json.JSONDecodeError:
                    logger.error(f"JSON decode error in file {file_path}. Resetting counter to 0.", exc_info=True)
                    value = 0
                value += 1
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(value, f)
                return value
        except IOError as e:
            logger.error(f"Error incrementing file {file_path}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error incrementing file {file_path}: {e}", exc_info=True)
            return None

    def set_result_and_progress(self, task_id, result, progress):
        result_key = f"result:{task_id}"
        progress_key = f"progress:{task_id}"
        result_path = self._get_file_path(result_key)
        progress_path = self._get_file_path(progress_key)
        
        logger.info(f"Setting result for task {task_id} at path: {result_path}")
        logger.info(f"Result content: {result}")
        
        # Validate result before setting
        try:
            json.dumps(result)
        except (TypeError, OverflowError) as e:
            logger.error(f"Result for task '{task_id}' is not JSON serializable: {e}", exc_info=True)
            # Modify the result to make it serializable if necessary
            result = {
                'status': 'error',
                'message': 'Result contains non-serializable data.',
                'errors': []
            }

        self.set(result_key, result)
        
        logger.info(f"Setting progress for task {task_id} at path: {progress_path}")
        self.set(progress_key, progress)
        
        logger.info(f"Verifying result was set for task {task_id}")
        stored_result = self.get(result_key)
        if stored_result is None:
            logger.error(f"Failed to store result for task {task_id}")
        else:
            logger.info(f"Successfully verified result for task {task_id}: {stored_result}")

# No module-level initialization of file_storage
# All instances will be created within functions to ensure proper process handling

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

def fetch_pdfs_from_drive_folder(folder_id, drive_service, task_id, file_storage):
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
    file_storage.set(f"progress:{task_id}:total", total_pdfs)

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
            file_storage.set(f"progress:{task_id}", round(progress_value, 1))
            logger.info(f"Fetched {processed_pdfs}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")
        except HttpError as e:
            logger.error(f"Failed to fetch PDF {file['name']}: {e}")
            continue  # Skip this file and continue with others

    return pdf_files_data

def normalize_name(name):
    """
    Normalize names by replacing lowercase 'n' followed by space(s) with 'Ñ',
    converting to uppercase, removing accents, and stripping whitespace.
    """
    if not name:
        return ''
    
    # Step 1: Replace lowercase 'n' followed by space(s) with 'Ñ'
    # Example: 'MU n OZ' -> 'MU ÑOZ'
    name = re.sub(r'n\s+', 'Ñ', name)
    
    # Step 2: Convert to uppercase to ensure consistency
    name = name.upper()
    
    # Step 3: Remove accents from characters
    name = unicodedata.normalize('NFD', name)
    name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
    
    # Step 4: Replace multiple spaces with a single space
    name = re.sub(r'\s+', ' ', name)
    
    # Step 5: Strip leading and trailing whitespace
    name = name.strip()
    
    return name

def process_pdfs_in_folder(folder_id, excel_file_content, excel_filename, sheets_file_id,
                           drive_service, sheets_service, folder_ids, main_folder_id, task_id):
    """
    Main function to process PDFs: fetch, extract information, pair, merge, and update sheets.
    Additionally, collects error data and creates an Excel file for PDFs with errors.
    """
    # Initialize file-based storage in the child process
    BASE_STORAGE_PATH = os.environ.get('STORAGE_PATH', '/tmp')
    file_storage = FileBasedStorage(base_path=BASE_STORAGE_PATH)
    logger.info(f"File storage initialized in process_pdfs_in_folder. Base path: {file_storage.base_path}")

    try:
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"File storage base path: {file_storage.base_path}")
        logger.info(f"File storage base path exists: {os.path.exists(file_storage.base_path)}")
        logger.info(f"File storage base path is writable: {os.access(file_storage.base_path, os.W_OK)}")
        
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
        file_storage.set(f"progress:{task_id}", 10)
        logger.info("Progress set to 10% after uploading Excel file.")

        # Fetch PDFs
        pdf_files_data = fetch_pdfs_from_drive_folder(folder_id, drive_service, task_id, file_storage)
        logger.info(f"Total PDFs fetched for processing: {len(pdf_files_data)}")
        total_pdfs = len(pdf_files_data)
        file_storage.set(f"progress:{task_id}:total", total_pdfs)

        if total_pdfs == 0:
            logger.warning(f"No PDFs found in folder {folder_id}.")
            result = {
                'status': 'success',
                'message': 'No PDFs found to process.',
                'errors': []
            }
            file_storage.set(f"progress:{task_id}", 99.9)
            file_storage.set(f"result:{task_id}", result)
            time.sleep(0.5)
            file_storage.set(f"progress:{task_id}", 100)
            logger.info("Progress set to 100% as no PDFs were found.")
            return

        # Prepare for multiprocessing
        manager = multiprocessing.Manager()
        pdf_info_list = manager.list()
        errors = manager.list()
        error_data = manager.list()
        error_files_set = manager.dict()

        # Initialize progress tracking
        file_storage.set(f"progress:{task_id}:completed_extraction", 0)
        logger.info("Initialized multiprocessing manager and progress tracking for PDF extraction.")

        # Create a partial function with fixed arguments, including file_storage
        extract_pdf_info_partial = partial(
            extract_pdf_info,
            pdf_info_list=pdf_info_list,
            errors=errors,
            error_data=error_data,
            error_files_set=error_files_set,
            drive_service=drive_service,
            folder_ids=folder_ids,
            task_id=task_id,
            total_pdfs=total_pdfs,
            file_storage=file_storage  # Pass file_storage as an argument
        )

        # Use a Pool for multiprocessing
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            for result in pool.imap_unordered(extract_pdf_info_partial, pdf_files_data):
                if result:
                    logger.info(f"Processed PDF: {result}")

        # Ensure final progress is set to 60% after extraction
        file_storage.set(f"progress:{task_id}", 60)
        logger.info("Extraction completed. Progress set to 60%.")

        # Convert manager lists to regular lists
        pdf_info_list = list(pdf_info_list)
        errors = list(errors)
        error_data = list(error_data)
        error_files_set = dict(error_files_set)

        logger.info("PDF extraction phase completed. Starting pairing process.")
        logger.info(f"Number of PDFs extracted: {len(pdf_info_list)}")

        try:
            logger.info("Starting PDF pairing process")
            pairs, pairing_errors = pair_pdfs(pdf_info_list, folder_ids['PDFs con Error'], drive_service, error_data, error_files_set)
            logger.info(f"PDF pairing completed. {len(pairs)} pairs created.")
            errors.extend(pairing_errors)

            logger.info("Starting to process PDF pairs")
            total_pairs = len(pairs)
            if total_pairs == 0:
                logger.warning("No PDF pairs to process.")
            else:
                processed_pairs = 0
                batch_updates = []

                for index, pair in enumerate(pairs):
                    try:
                        merged_pdf = merge_pdfs([pair['pdfs'][0], pair['pdfs'][1]])

                        if merged_pdf:
                            client_unique = update_google_sheet(
                                excel_file_id,
                                pair['info']['name'],
                                pair['info'].get('folio_number'),
                                pair['info'].get('oficina'),
                                sheets_service,
                                batch_updates=batch_updates
                            )

                            if client_unique:
                                file_name = f"{client_unique} {pair['info']['name']}.pdf"
                                upload_file_to_drive(merged_pdf, folder_ids['PDFs Unificados'], drive_service, file_name)
                                logger.info(f"Merged PDF for {pair['info']['name']} uploaded to 'PDFs Unificados'")
                                processed_pairs += 1
                            else:
                                for pdf_content, pdf_filename in zip(pair['pdfs'], pair['pdf_filenames']):
                                    error_message = f"Client '{pair['info']['name']}' not found in Excel."
                                    errors.append({'file_name': pdf_filename, 'message': error_message})
                                    logger.warning(error_message)
                                    error_entry = {
                                        'DOCUMENTO': pdf_filename,
                                        'NOMBRE_CTE': pair['info']['name'],
                                        'FOLIO DE REGISTRO': pair['info'].get('folio_number', ''),
                                        'OFICINA DE CORRESPONDENCIA': pair['info'].get('oficina', ''),
                                        'ERROR': error_message
                                    }
                                    error_data.append(error_entry)
                                    error_files_set[pdf_filename] = True
                                    upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)
                        else:
                            for pdf_content, pdf_filename in zip(pair['pdfs'], pair['pdf_filenames']):
                                error_message = f"Failed to merge PDFs for {pair['info']['name']}"
                                errors.append({'file_name': pdf_filename, 'message': error_message})
                                logger.warning(error_message)
                                error_entry = {
                                    'DOCUMENTO': pdf_filename,
                                    'NOMBRE_CTE': pair['info']['name'],
                                    'FOLIO DE REGISTRO': pair['info'].get('folio_number', ''),
                                    'OFICINA DE CORRESPONDENCIA': pair['info'].get('oficina', ''),
                                    'ERROR': error_message
                                }
                                error_data.append(error_entry)
                                error_files_set[pdf_filename] = True
                                upload_file_to_drive(io.BytesIO(pdf_content), folder_ids['PDFs con Error'], drive_service, pdf_filename)

                        progress_value = 70 + ((index + 1) / total_pairs * 29)  # Changed to 29 to leave room for 99.9%
                        file_storage.set(f"progress:{task_id}", round(progress_value, 1))
                        logger.info(f"Processed pair {index + 1}/{total_pairs}. Progress: {progress_value:.1f}%")

                    except Exception as pair_error:
                        logger.error(f"Error processing pair {index + 1}: {str(pair_error)}", exc_info=True)
                        errors.append({
                            'pair_index': index + 1,
                            'message': f"Error processing pair: {str(pair_error)}"
                        })

                logger.info("Completed processing all PDF pairs")

                if batch_updates:
                    try:
                        batch_update_google_sheet(excel_file_id, batch_updates, sheets_service)
                        logger.info(f"Batch update to Google Sheets completed with {len(batch_updates)} updates.")
                    except Exception as e:
                        logger.error(f"Error during batch update to Google Sheets: {e}", exc_info=True)
                else:
                    logger.info("No updates to perform on Google Sheets.")

        except Exception as e:
            logger.error(f"Error during PDF pairing: {str(e)}", exc_info=True)
            error_result = {'status': 'error', 'message': f"PDF pairing failed: {str(e)}"}
            file_storage.set(f"progress:{task_id}", 99.9)
            file_storage.set(f"result:{task_id}", error_result)
            time.sleep(0.5)
            file_storage.set(f"progress:{task_id}", 100)
            return

        # Create Excel file for PDFs with errors
        if error_data:
            try:
                df_errors = pd.DataFrame(error_data, columns=[
                    'DOCUMENTO', 'NOMBRE_CTE', 'FOLIO DE REGISTRO', 'OFICINA DE CORRESPONDENCIA', 'ERROR'
                ])
                df_errors.drop_duplicates(subset=['DOCUMENTO', 'ERROR'], inplace=True)
                excel_buffer = io.BytesIO()
                df_errors.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                excel_file_name = 'PDFs con Error.xlsx'
                upload_file_to_drive(
                    excel_buffer,
                    main_folder_id,
                    drive_service,
                    excel_file_name,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                logger.info(f"Excel file '{excel_file_name}' uploaded to process folder with ID: {main_folder_id}")
            except Exception as e:
                logger.error(f"Error creating or uploading Excel file for errors: {e}", exc_info=True)
        else:
            logger.info("No error data to write to Excel.")

        # Prepare and store the final result
        result = {
            'status': 'success',
            'message': f'Processed {processed_pairs} pairs with {len(errors)} errors.',
            'errors': list(errors)  # Convert manager.list() to regular list
        }

        # Set progress to 99.9% before writing the result
        file_storage.set(f"progress:{task_id}", 99.9)
        logger.info(f"Progress set to 99.9% for task {task_id}")

        # Write the result
        file_storage.set(f"result:{task_id}", result)
        logger.info(f"Result set for task {task_id}")

        # Add a small delay
        time.sleep(0.5)

        # Verify the result was written correctly
        stored_result = file_storage.get(f"result:{task_id}")
        if stored_result is None:
            logger.error(f"Failed to store result for task {task_id}")
        else:
            logger.info(f"Successfully verified result for task {task_id}: {stored_result}")
            # Only set progress to 100% if the result was successfully stored
            file_storage.set(f"progress:{task_id}", 100)
            logger.info(f"Progress set to 100% for task {task_id}")

        logger.info("Processing completed successfully.")

    except Exception as e:
        logger.error(f"Error processing PDFs: {e}", exc_info=True)
        error_result = {'status': 'error', 'message': str(e)}
        
        # Set progress to 99.9% before writing the error result
        file_storage.set(f"progress:{task_id}", 99.9)
        logger.info(f"Progress set to 99.9% for task {task_id} due to an error")
        
        # Write the error result
        file_storage.set(f"result:{task_id}", error_result)
        logger.info(f"Error result set for task {task_id}")
        
        time.sleep(0.5)
        
        # Verify the error result was written correctly
        stored_result = file_storage.get(f"result:{task_id}")
        if stored_result is None:
            logger.error(f"Failed to store error result for task {task_id}")
        else:
            logger.info(f"Successfully verified error result for task {task_id}: {stored_result}")
            # Only set progress to 100% if the error result was successfully stored
            file_storage.set(f"progress:{task_id}", 100)
            logger.info(f"Progress set to 100% for task {task_id} after error")

    finally:
        logger.info("PDF processing completed (success or failure).")

def extract_pdf_info(pdf_data, pdf_info_list, errors, error_data, error_files_set, 
                     drive_service, folder_ids, task_id, total_pdfs, file_storage):
    """
    Extract information from a single PDF and update progress.
    Returns the filename if processed successfully, otherwise None.
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
            logger.error(f"Error uploading original PDF '{pdf_filename}' to 'PDFs Originales': {e}", exc_info=True)

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
                logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)
            return None

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
                logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)
            return None

        # Log the info extracted before normalization
        logger.debug(f"Extracted info before normalization: {info}")

        # Normalize the extracted name
        info['normalized_name'] = normalize_name(info.get('name', ''))

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
            # Add to error_data if not already added
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
                logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)
            return None
        else:
            # All critical fields are present
            pdf_info_list.append({
                'file_name': pdf_filename,
                'content': pdf_content,
                'info': info
            })

        # Log successful processing
        logger.info(f"Successfully extracted information from PDF: {pdf_filename}")

        # Update progress
        completed = file_storage.incr(f"progress:{task_id}:completed_extraction")
        if completed is not None:
            progress_value = 30 + ((completed / total_pdfs) * 30)  # Scale to 30-60%
            file_storage.set(f"progress:{task_id}", round(progress_value, 1))
            logger.info(f"Extracted info from {completed}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")
        else:
            logger.error(f"Failed to update progress for task {task_id}.")

        return pdf_filename  # Return the filename to indicate successful processing

    except Exception as e:
        logger.error(f"Error processing PDF {pdf_filename}: {e}", exc_info=True)
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
            logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)
        return None
    finally:
        # Ensure progress is updated even if an error occurs
        try:
            completed = file_storage.get(f"progress:{task_id}:completed_extraction") or 0
            progress_value = 30 + ((completed / total_pdfs) * 30)
            file_storage.set(f"progress:{task_id}", round(progress_value, 1))
            logger.info(f"Progress updated to {progress_value:.1f}% after processing '{pdf_filename}'.")
        except Exception as e:
            logger.error(f"Error updating progress for task {task_id}: {e}", exc_info=True)

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
        normalized_name = pdf_info['info'].get('normalized_name')
        if pdf_type == 'ACUSE' and normalized_name:
            acuse_dict[normalized_name].append(pdf_info)
        elif pdf_type == 'DEMANDA' and normalized_name:
            demanda_dict[normalized_name].append(pdf_info)
        else:
            # If type is missing but name is present, treat it as ACUSE
            if normalized_name:
                acuse_dict[normalized_name].append(pdf_info)
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
                        logger.error(f"Error uploading duplicate ACUSE '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
                        logger.error(f"Error uploading duplicate DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
                        logger.error(f"Error uploading ACUSE '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
                        logger.error(f"Error uploading DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

    # Pair PDFs based on the name
    for name in names_to_pair:
        acuse_list = acuse_dict[name]
        demanda_list = demanda_dict[name]

        if len(acuse_list) == 1 and len(demanda_list) == 1:
            acuse_pdf = acuse_list[0]
            demanda_pdf = demanda_list[0]

            # Merge info from both DEMANDA and ACUSE, ensuring the name from DEMANDA is retained
            combined_info = {**acuse_pdf['info'], **demanda_pdf['info']}  # Ensure DEMANDA info overrides ACUSE
            combined_info['name'] = demanda_pdf['info']['name']  # Explicitly set the correct name from DEMANDA

            pairs.append({
                'name': combined_info['name'],  # Use the correct name for final processing
                'pdfs': [acuse_pdf['content'], demanda_pdf['content']],
                'info': combined_info,
                'file_name': demanda_pdf['file_name'],  # Assuming DEMANDA is the primary file
                'pdf_filenames': [acuse_pdf['file_name'], demanda_pdf['file_name']]
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
                        logger.error(f"Error uploading unexpected ACUSE '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
                        logger.error(f"Error uploading DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
                        logger.error(f"Error uploading DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
                        logger.error(f"Error uploading ACUSE '{pdf_filename}' to 'PDFs con Error': {e}", exc_info=True)

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
        logger.info(f"Cleaned DEMANDA Text:\n{text[:200]}")

        # Adjusted regex pattern to match the text structure (with dot handling for names like MA. DEL REFUGIO)
        nombre_match = re.search(
            r'VS\s*([A-ZÁÉÍÓÚÑÜ\s.]+)\s*MEDIOS PREPARATORIOS',
            text,
            re.UNICODE | re.IGNORECASE
        )

        if not nombre_match:
            # Try alternative patterns if the first one doesn't match
            nombre_match = re.search(
                r'VS\s*([A-ZÁÉÍÓÚÑÜ\s.]+)\s*ESCRITO INICIAL',
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
        logger.error(f"Error during extraction (DEMANDA): {e}", exc_info=True)
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
        logger.info(f"Extracted Text from ACUSE PDF:\n{text[:200]}")

        # Extract 'nombre' using adjusted regex to exclude 'ANEXOS' and allow dots in names
        nombre_match = re.search(
            r'BAZ\s*VS\s*([\wÁÉÍÓÚÑÜáéíóúñü\s.]+?)(?=\s*ANEXOS\.pdf|\s*ANEXOS\s|\.pdf|\s*$)',
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
        logger.error(f"Error during extraction (ACUSE): {e}", exc_info=True)
        return None

def post_process_text(text):
    """
    Apply corrections to text formatting.
    """
    # Replace known concatenated words with proper spacing
    text = text.replace("Oficinade", "Oficina de")
    text = text.replace("Foliode", "Folio de")
    text = text.replace("Estadode", "Estado de")
    text = text.replace("elEstado", "el Estado")
    text = text.replace("Residenciade", "Residencia de")
    
    # General correction: Insert space between a lowercase letter followed by an uppercase letter
    text = re.sub(r'([a-záéíóúñü])([A-ZÁÉÍÓÚÑÜ])', r'\1 \2', text)
    
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
        logger.error(f"Error extracting text from PDF: {e}", exc_info=True)
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
