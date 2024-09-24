# backend/pdf_handler.py

import io
import re
import logging
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
from backend.task_manager import progress_data  # Import shared progress_data
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@retry(
    retry=retry_if_exception_type(HttpError),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True
)
def list_drive_files(drive_service, folder_id, page_token):
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
    Updates progress_data during the fetching process.
    """
    pdf_files_data = []
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
    if total_pdfs == 0:
        logger.warning(f"No PDFs found in folder {folder_id}.")
        return pdf_files_data

    # Download PDFs with progress updates
    processed_pdfs = 0
    for file in files:
        try:
            # Download the PDF content
            file_content = download_drive_file(drive_service, file['id'])
            pdf_files_data.append({
                'filename': file['name'],
                'content': file_content
            })
            processed_pdfs += 1
            # Update progress (0% to 10%)
            progress_value = (processed_pdfs / total_pdfs) * 10  # Allocating 10% for fetching
            progress_data[task_id] = progress_value
            logger.info(f"Fetched {processed_pdfs}/{total_pdfs} PDFs. Progress: {progress_value}%")
        except HttpError as e:
            logger.error(f"Failed to fetch PDF {file['name']}: {e}")
            raise e  # Retry on HttpError

    return pdf_files_data


def process_pdfs_in_folder(pdf_files_data, excel_file_content, excel_filename, sheets_file_id,
                           drive_service, sheets_service, folder_ids, main_folder_id, task_id):
    try:
        # Initial progress after fetching PDFs
        initial_progress = progress_data.get(task_id, 0)
        if initial_progress < 10:
            initial_progress = 10  # Ensure we start from 10%

        # Phase 1: Process PDFs (10% to 60%)
        total_pdfs = len(pdf_files_data)
        processed_pdfs = 0

        pdf_info_list = []
        errors = []  # Collect errors here

        logger.info(f"Starting processing of {total_pdfs} PDFs")

        # Get folder IDs for organizing processed PDFs
        originals_folder_id = folder_ids['PDFs Originales']
        error_folder_id = folder_ids['PDFs con Error']
        unified_folder_id = folder_ids['PDFs Unificados']

        # Upload Excel file to Google Drive if provided
        if excel_file_content:
            excel_file_stream = io.BytesIO(excel_file_content)
            excel_file_stream.seek(0)
            excel_file_id = upload_excel_to_drive(
                excel_file_stream, excel_filename, drive_service, parent_folder_id=main_folder_id)
        else:
            excel_file_id = sheets_file_id

        # Initialize batch updates list for Google Sheets
        batch_updates = []

        for pdf_data in pdf_files_data:
            pdf_content = pdf_data['content']
            pdf_filename = pdf_data['filename']
            pdf_stream = io.BytesIO(pdf_content)
            logger.info(f"Processing PDF: {pdf_filename}")

            # Upload original PDF to "PDFs Originales"
            try:
                upload_file_to_drive(io.BytesIO(pdf_content), originals_folder_id, drive_service, pdf_filename)
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
                    logger.warning(f"Could not extract DEMANDA info from {pdf_filename}. Moving to error folder.")
                    # Upload to "PDFs con Error"
                    try:
                        upload_file_to_drive(io.BytesIO(pdf_content), error_folder_id, drive_service, pdf_filename)
                        logger.info(f"Uploaded {pdf_filename} to 'PDFs con Error'")
                    except Exception as e:
                        logger.error(f"Error uploading PDF '{pdf_filename}' to 'PDFs con Error': {e}")
                    # Collect error information
                    errors.append({
                        'file_name': pdf_filename,
                        'message': f"Could not extract information from PDF: {pdf_filename}"
                    })
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
            processed_pdfs += 1
            progress_value = 10 + ((processed_pdfs / total_pdfs) * 50)  # Scale to 10-60%
            progress_data[task_id] = progress_value  # Update shared progress_data
            logger.info(f"Processed {processed_pdfs}/{total_pdfs}. Progress: {progress_value}%")

        # Pair PDFs based on names and types
        pairs, pairing_errors = pair_pdfs(pdf_info_list, error_folder_id, drive_service)
        errors.extend(pairing_errors)  # Add pairing errors to the errors list

        # Phase 2: Process Pairs (60% to 100%)
        total_pairs = len(pairs)
        processed_pairs = 0

        if total_pairs == 0:
            total_pairs = 1  # Prevent division by zero

        # Initialize list to collect batch updates for Google Sheets
        batch_updates = []

        for pair in pairs:
            merged_pdf = merge_pdfs([pair['pdfs'][0], pair['pdfs'][1]])
            if merged_pdf:
                # Collect update information for batch update
                client_unique = update_google_sheet(
                    excel_file_id,
                    pair['name'],
                    pair['info'].get('folio_number'),
                    pair['info'].get('oficina'),
                    sheets_service,
                    batch_updates=batch_updates  # Pass the batch_updates list
                )

                if client_unique:
                    file_name = f"{client_unique} {pair['name']}.pdf"
                    upload_file_to_drive(merged_pdf, unified_folder_id, drive_service, file_name)
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

            # Update progress after processing each pair
            processed_pairs += 1
            progress_value = 60 + ((processed_pairs / total_pairs) * 40)  # Scale to 60-100%
            progress_data[task_id] = progress_value  # Continue updating shared progress_data
            logger.info(f"Processed pair {processed_pairs}/{total_pairs}. Progress: {progress_value}%")

        # After processing all pairs, perform batch update to Google Sheets
        if batch_updates:
            batch_update_google_sheet(excel_file_id, batch_updates, sheets_service)
            logger.info(f"Batch update to Google Sheets completed with {len(batch_updates)} updates.")
        else:
            logger.info("No updates to perform on Google Sheets.")

        # Finalize progress
        progress_data[task_id] = 100  # Mark the task as complete
        logger.info("PDF processing complete. Final progress: 100%")

        # Handle errors
        if errors:
            logger.warning("Some PDFs could not be processed.")
            return {
                'status': 'success',
                'message': 'Processing complete with some errors.',
                'errors': errors
            }
        else:
            return {'status': 'success', 'message': 'Processing complete.'}

    except Exception as e:
        logger.error(f"Error processing PDFs: {str(e)}")
        progress_data[task_id] = 100  # Ensure progress is marked as complete
        return {'status': 'error', 'message': str(e)}
        
def extract_demanda_information(pdf_stream):
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            text += page.extract_text()

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
                'type': 'DEMANDA'
            }
        else:
            logger.warning(f"Could not extract name from DEMANDA PDF.")
            return None
    except Exception as e:
        logger.error(f"Error during extraction (DEMANDA): {e}")
        return None


def extract_acuse_information(pdf_stream):
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            text += page.extract_text()

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
                'type': 'ACUSE'
            }
        else:
            logger.warning(f"Could not extract all required fields from ACUSE PDF.")
            return None
    except Exception as e:
        logger.error(f"Error during extraction (ACUSE): {e}")
        return None


def post_process_text(text):
    # Apply corrections to text formatting
    text = text.replace("Oficinade", "Oficina de")
    text = text.replace("Foliode", "Folio de")
    text = add_missing_spaces(text)
    return text


def add_missing_spaces(text):
    # Add spaces where needed between words
    return re.sub(r'([a-z])([A-ZÁÉÍÓÚÑÜ])', r'\1 \2', text)


def extract_full_text(pdf_stream):
    """Extract full text from the PDF for debugging purposes."""
    try:
        reader = PdfReader(pdf_stream)
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text()
        return full_text
    except Exception as e:
        logger.error(f"Error extracting full text from PDF: {e}")
        return ""


def pair_pdfs(pdf_info_list, error_folder_id, drive_service):
    """
    Pairs ACUSE and DEMANDA PDFs based on the extracted names and uploads unmatched PDFs to 'PDFs con Error'.
    """
    paired_pdfs = []
    errors = []

    # Group PDFs by types
    acuse_pdfs = [info for info in pdf_info_list if info['info']['type'] == 'ACUSE']
    demanda_pdfs = [info for info in pdf_info_list if info['info']['type'] == 'DEMANDA']

    # Copy of DEMANDA PDFs to keep track of unmatched ones
    unmatched_demandas = demanda_pdfs.copy()

    # Pair by matching names
    for acuse in acuse_pdfs:
        matched_demandas = [
            demanda for demanda in unmatched_demandas
            if normalize_text(demanda['info']['name']) == normalize_text(acuse['info']['name'])
        ]

        if matched_demandas:
            paired_pdfs.append({
                'pdfs': [acuse['content'], matched_demandas[0]['content']],
                'file_name': acuse['file_name'],
                'name': acuse['info']['name'],
                'info': acuse['info']
            })
            unmatched_demandas.remove(matched_demandas[0])  # Remove matched DEMANDA
        else:
            errors.append({
                'file_name': acuse['file_name'],
                'message': f"No matching DEMANDA found for ACUSE: {acuse['info']['name']}"
            })
            # Upload unmatched ACUSE PDF to 'PDFs con Error'
            try:
                upload_file_to_drive(io.BytesIO(acuse['content']), error_folder_id, drive_service, acuse['file_name'])
                logger.info(f"Uploaded unmatched ACUSE '{acuse['file_name']}' to 'PDFs con Error'")
            except Exception as e:
                logger.error(f"Error uploading unmatched ACUSE '{acuse['file_name']}' to 'PDFs con Error': {e}")

    # Handle unmatched DEMANDA PDFs
    for demanda in unmatched_demandas:
        errors.append({
            'file_name': demanda['file_name'],
            'message': f"No matching ACUSE found for DEMANDA: {demanda['info']['name']}"
        })
        # Upload unmatched DEMANDA PDF to 'PDFs con Error'
        try:
            upload_file_to_drive(io.BytesIO(demanda['content']), error_folder_id, drive_service, demanda['file_name'])
            logger.info(f"Uploaded unmatched DEMANDA '{demanda['file_name']}' to 'PDFs con Error'")
        except Exception as e:
            logger.error(f"Error uploading unmatched DEMANDA '{demanda['file_name']}' to 'PDFs con Error': {e}")

    return paired_pdfs, errors


def merge_pdfs(pdfs):
    """Merge two PDFs (ACUSE and DEMANDA)."""
    writer = PdfWriter()
    for pdf_content in pdfs:
        reader = PdfReader(io.BytesIO(pdf_content))
        for page in reader.pages:
            writer.add_page(page)

    merged_pdf = io.BytesIO()
    writer.write(merged_pdf)
    merged_pdf.seek(0)
    return merged_pdf
