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
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import pandas as pd
import unicodedata

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

def fetch_pdfs_from_drive_folder(folder_id, drive_service, task_id, progress_dict):
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
    progress_dict['total_pdfs'] = total_pdfs

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
            progress_dict['progress'] = round(progress_value, 2)
            logger.info(f"Fetched {processed_pdfs}/{total_pdfs} PDFs. Progress: {progress_value:.1f}%")
        except HttpError as e:
            logger.error(f"Failed to fetch PDF {file['name']}: {e}")
            continue  # Skip this file and continue with others

    return pdf_files_data

def normalize_name(name):
    """
    Normalize names by converting to uppercase, removing accents, and stripping whitespace.
    """
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
                           drive_service, sheets_service, folder_ids, main_folder_id, task_id, progress_dict, result_dict,
                           pdf_info_list, errors, error_data, error_files_set):
    """
    Main function to process PDFs: fetch, extract information, pair, merge, and update sheets.
    Additionally, collects error data and creates an Excel file for PDFs with errors.

    Parameters:
        folder_id (str): ID of the Google Drive folder containing PDFs.
        excel_file_content (bytes): Content of the uploaded Excel file.
        excel_filename (str): Name of the uploaded Excel file.
        sheets_file_id (str): ID of an existing Google Sheets file.
        drive_service: Initialized Google Drive service.
        sheets_service: Initialized Google Sheets service.
        folder_ids (dict): Dictionary containing IDs of relevant subfolders.
        main_folder_id (str): ID of the main process-specific folder in Google Drive.
        task_id (str): Unique identifier for the processing task.
        progress_dict (dict): Shared dictionary for tracking progress.
        result_dict (dict): Shared dictionary for storing the final result.
        pdf_info_list (multiprocessing.Manager().list): Shared list to store extracted PDF info.
        errors (multiprocessing.Manager().list): Shared list to store error details.
        error_data (multiprocessing.Manager().list): Shared list to collect error data for Excel.
        error_files_set (multiprocessing.Manager().dict): Shared dict to track processed error files.
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
        progress_dict['progress'] = 10
        progress_dict['status'] = 'Excel uploaded'

        # Fetch PDFs
        pdf_files_data = fetch_pdfs_from_drive_folder(folder_id, drive_service, task_id, progress_dict)
        logger.info(f"Total PDFs fetched for processing: {len(pdf_files_data)}")
        total_pdfs = len(pdf_files_data)
        progress_dict['total_pdfs'] = total_pdfs

        if total_pdfs == 0:
            logger.warning(f"No PDFs found in folder {folder_id}.")
            result = {
                'status': 'success',
                'message': 'No PDFs found to process.',
                'errors': []
            }
            result_dict['result'] = result
            progress_dict['progress'] = 100
            progress_dict['status'] = 'Completed with no PDFs'
            return

        # Initialize extraction progress
        progress_dict['completed_extraction'] = 0

        # Create a partial function with fixed arguments
        extract_pdf_info_partial = partial(
            extract_pdf_info,
            pdf_info_list=pdf_info_list,
            errors=errors,
            error_data=error_data,  # Pass error_data
            error_files_set=error_files_set,  # Pass error_files_set
            drive_service=drive_service,
            folder_ids=folder_ids,
            task_id=task_id,
            progress_dict=progress_dict
        )

        # Process PDFs in parallel to extract info
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        pool.map(extract_pdf_info_partial, pdf_files_data)
        pool.close()
        pool.join()

        # Update progress to 60% after extraction
        progress_dict['progress'] = 60
        progress_dict['status'] = 'Extraction completed'

        # Pair PDFs based on names and types
        pairs, pairing_errors = pair_pdfs(pdf_info_list, folder_ids['PDFs con Error'], drive_service, error_data, error_files_set)
        errors.extend(pairing_errors)

        # Update progress after pairing
        progress_dict['progress'] = 70
        progress_dict['status'] = 'Pairing completed'

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
            progress_dict['progress'] = round(progress_value, 2)
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
                'CLIENTE_UNICO',
                'FOLIO DE REGISTRO',
                'OFICINA_DE_CORRESPONDENCIA'
            ])

            # Ensure 'CLIENTE_UNICO' is empty
            df_errors['CLIENTE_UNICO'] = ''

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

        # Store the result in the shared dictionary before setting progress to 100%
        result_dict['result'] = result

        # Update overall progress to 100%
        progress_dict['progress'] = 100
        progress_dict['status'] = 'Completed'

    except Exception as e:
        # Handle exceptions and update progress dictionary
        error_result = {'status': 'error', 'message': str(e)}
        result_dict['result'] = error_result
        progress_dict['progress'] = 100
        progress_dict['status'] = 'Error'
        logger.error(f"Error processing PDFs: {str(e)}")


def extract_pdf_info(pdf_data, pdf_info_list, errors, error_data, error_files_set, drive_service, folder_ids, task_id, progress_dict):
    """
    Extract information from a single PDF file.
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
                'CLIENTE_UNICO': '',
                'FOLIO DE REGISTRO': '',
                'OFICINA_DE_CORRESPONDENCIA': ''
            }
            # Add to error_data if not already added
            if pdf_filename not in error_files_set:
                error_data.append(partial_info)
                error_files_set[pdf_filename] = True
                errors.append({
                    'file_name': pdf_filename,
                    'message': "Unable to classify PDF."
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
                'CLIENTE_UNICO': '',
                'FOLIO DE REGISTRO': '',
                'OFICINA_DE_CORRESPONDENCIA': ''
            }
            # Add to error_data if not already added
            if pdf_filename not in error_files_set:
                error_data.append(partial_info)
                error_files_set[pdf_filename] = True
                errors.append({
                    'file_name': pdf_filename,
                    'message': "Unable to extract valid information from PDF."
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
                'CLIENTE_UNICO': '',
                'FOLIO_DE_REGISTRO': info.get('folio_number', ''),
                'OFICINA_DE_CORRESPONDENCIA': info.get('oficina', '')
            }
            if pdf_filename not in error_files_set:
                error_data.append(partial_info)
                error_files_set[pdf_filename] = True
                errors.append({
                    'file_name': pdf_filename,
                    'message': f"Missing critical fields: {', '.join(missing_fields)}"
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
        completed = progress_dict.get('completed_extraction', 0) + 1
        progress_dict['completed_extraction'] = completed
        total_pdfs = progress_dict.get('total_pdfs', 1)
        progress_value = 30 + ((completed / total_pdfs) * 30)  # Scale to 30-60%
        progress_dict['progress'] = round(progress_value, 2)
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
            'CLIENTE_UNICO': '',
            'FOLIO_DE_REGISTRO': '',
            'OFICINA_DE_CORRESPONDENCIA': ''
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
    Pairs ACUSE and DEMANDA PDFs based on the extracted names and uploads unmatched PDFs to 'PDFs con Error'.
    Also collects error data for unmatched PDFs.
    """
    acuse_dict = {}
    demanda_dict = {}
    pairs = []
    errors = []

    # Separate PDFs into ACUSE and DEMANDA
    for pdf_info in pdf_info_list:
        pdf_type = pdf_info['info'].get('type')  # Ensure the key is 'type'
        name = pdf_info['info'].get('name')
        if pdf_type == 'ACUSE' and name:
            acuse_dict[name] = pdf_info
        elif pdf_type == 'DEMANDA' and name:
            demanda_dict[name] = pdf_info
        else:
            # If type is missing but name is present, we can try to pair it
            if name:
                # Decide where to place it based on available info
                # For simplicity, treat it as ACUSE
                acuse_dict[name] = pdf_info
            else:
                if pdf_info['file_name'] not in error_files_set:
                    errors.append({
                        'file_name': pdf_info['file_name'],
                        'message': f"Unknown or missing PDF type and name for {pdf_info['file_name']}"
                    })
                    error_files_set[pdf_info['file_name']] = True
                    logger.warning(f"Unknown or missing PDF type and name for {pdf_info['file_name']}")

    # Pair PDFs based on the name
    for name in demanda_dict.keys():
        if name in acuse_dict:
            # Merge info from both DEMANDA and ACUSE
            combined_info = {**demanda_dict[name]['info'], **acuse_dict[name]['info']}
            pairs.append({
                'name': name,
                'pdfs': [acuse_dict[name]['content'], demanda_dict[name]['content']],
                'info': combined_info,
                'file_name': demanda_dict[name]['file_name']
            })
        else:
            # DEMANDA without matching ACUSE
            pdf_filename = demanda_dict[name]['file_name']
            if pdf_filename not in error_files_set:
                errors.append({
                    'file_name': pdf_filename,
                    'message': f"No matching ACUSE found for DEMANDA: {name}"
                })
                logger.warning(f"No matching ACUSE found for DEMANDA: {name}")
                # Collect error data
                error_entry = {
                    'DOCUMENTO': pdf_filename,
                    'NOMBRE_CTE': demanda_dict[name]['info'].get('name', ''),
                    'CLIENTE_UNICO': '',
                    'FOLIO_DE_REGISTRO': demanda_dict[name]['info'].get('folio_number', ''),
                    'OFICINA_DE_CORRESPONDENCIA': demanda_dict[name]['info'].get('oficina', '')
                }
                error_data.append(error_entry)
                error_files_set[pdf_filename] = True
                # Optionally, upload DEMANDA to 'PDFs con Error'
                try:
                    upload_file_to_drive(io.BytesIO(demanda_dict[name]['content']), error_folder_id, drive_service, pdf_filename)
                    logger.info(f"Uploaded unmatched DEMANDA '{pdf_filename}' to 'PDFs con Error'")
                except Exception as e:
                    logger.error(f"Error uploading DEMANDA '{pdf_filename}' to 'PDFs con Error': {e}")

    for name in acuse_dict.keys():
        if name not in demanda_dict:
            # ACUSE without matching DEMANDA
            pdf_filename = acuse_dict[name]['file_name']
            if pdf_filename not in error_files_set:
                errors.append({
                    'file_name': pdf_filename,
                    'message': f"No matching DEMANDA found for ACUSE: {name}"
                })
                logger.warning(f"No matching DEMANDA found for ACUSE: {name}")
                # Collect error data
                error_entry = {
                    'DOCUMENTO': pdf_filename,
                    'NOMBRE_CTE': acuse_dict[name]['info'].get('name', ''),
                    'CLIENTE_UNICO': '',
                    'FOLIO_DE_REGISTRO': acuse_dict[name]['info'].get('folio_number', ''),
                    'OFICINA_DE_CORRESPONDENCIA': acuse_dict[name]['info'].get('oficina', '')
                }
                error_data.append(error_entry)
                error_files_set[pdf_filename] = True
                # Optionally, upload ACUSE to 'PDFs con Error'
                try:
                    upload_file_to_drive(io.BytesIO(acuse_dict[name]['content']), error_folder_id, drive_service, pdf_filename)
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
            r'BAZ\s*VS\s*([A-ZÁÉÍÓÚÑÜ\s]+?)(?=\s*ANEXOS\.pdf|\s*ANEXOS\s|\.pdf|\s*$)',
            text
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

        # If no name is found, return None (error handling)
        if not extracted_name:
            logger.warning("No name match found in ACUSE PDF.")
            return None

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
