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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            logger.info(f"Processing PDF: {pdf_file.filename}")

            # Attempt to extract "ACUSE" info first
            info = extract_acuse_information(io.BytesIO(pdf_content))
            if not info:
                logger.info(f"Could not extract ACUSE info from {pdf_file.filename}. Trying DEMANDA extraction.")
                # If not "ACUSE", try extracting "DEMANDA" info
                info = extract_demanda_information(io.BytesIO(pdf_content))
                if not info:
                    logger.warning(f"Could not extract DEMANDA info from {pdf_file.filename}. Printing full text for debug.")
                    full_text = extract_full_text(io.BytesIO(pdf_content))
                    logger.error(f"Full extracted text: {full_text[:2000]}...")  # Log first 2000 characters for debugging
                    # Move PDF to error folder
                    error_folder_id = get_or_create_folder('PDF Merger App/PDFs con error', drive_service)
                    upload_file_to_drive(io.BytesIO(pdf_content), error_folder_id, drive_service, pdf_file.filename)
                    continue

            pdf_info_list.append({
                'file_name': pdf_file.filename,
                'content': pdf_content,
                'info': info
            })

        # Pair PDFs based on names and "acuse" presence
        pairs, errors = pair_pdfs(pdf_info_list)

        # Process pairs
        error_files = []
        for pair in pairs:
            merged_pdf = merge_pdfs(pair['pdfs'])
            if merged_pdf:
                # Save merged PDF to Drive
                folder_id = get_or_create_folder('PDF Merger App/Merged PDFs', drive_service)

                # Update Google Sheets and get CLIENTE_UNICO for naming
                client_unique = update_google_sheet(
                    excel_file_id,
                    pair['name'],  # This is the NOMBRE_CTE extracted
                    pair['info'].get('folio_number'),
                    pair['info'].get('oficina'),
                    sheets_service
                )

                if client_unique:
                    # Use CLIENTE_UNICO and the name for naming the merged PDF
                    file_name = f"{client_unique}_{pair['name']}.pdf"
                    upload_file_to_drive(merged_pdf, folder_id, drive_service, file_name)
                else:
                    error_files.append({
                        'file_name': pair['name'],
                        'message': f"Client '{pair['name']}' not found in sheet."
                    })
            else:
                error_files.append({
                    'file_name': pair['name'],
                    'message': f"Failed to merge PDFs for {pair['name']}"
                })

        # Handle errors
        if errors or error_files:
            return {
                'status': 'error',
                'message': 'Some PDFs could not be processed.',
                'errors': error_files
            }
        else:
            return {'status': 'success'}

    except Exception as e:
        logger.error(f"Error processing PDFs: {str(e)}")
        return {'status': 'error', 'message': str(e)}


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
        logger.info(f"Extracted - Oficina: {oficina_match.group(0).strip() if oficina_match else 'None'}, "
                    f"Folio: {folio_match.group(1) if folio_match else 'None'}, "
                    f"Nombre: {nombre_match.group(1) if nombre_match else 'None'}")

        if oficina_match and folio_match and nombre_match:
            return {
                'oficina': oficina_match.group(0).strip(),
                'folio_number': folio_match.group(1).strip(),
                'name': nombre_match.group(1).strip(),
                'type': 'ACUSE'
            }
        else:
            logger.warning(f"Could not extract all required fields from ACUSE PDF.")
            return None
    except Exception as e:
        logger.error(f"Error during extraction (ACUSE): {e}")
        return None

def extract_demanda_information(pdf_stream):
    try:
        reader = PdfReader(pdf_stream)
        text = ""
        for page in reader.pages:
            text += page.extract_text()

        # Post-process the text for better extraction
        text = post_process_text(text)

        # Try different approaches to capture the name after "VS" in DEMANDA documents
        nombre_match = re.search(r'VS\s*(.*?)(?=\n|MEDIOS|\.|$)', text)

        # Log the extracted field
        logger.info(f"Extracted - Nombre (DEMANDA): {nombre_match.group(1) if nombre_match else 'None'}")

        if nombre_match:
            return {
                'name': nombre_match.group(1).strip(),
                'type': 'DEMANDA'
            }
        else:
            logger.warning(f"Could not extract name from DEMANDA PDF.")
            return None
    except Exception as e:
        logger.error(f"Error during extraction (DEMANDA): {e}")
        return None

def post_process_text(text):
    # Apply corrections to text formatting
    text = text.replace("Oficinade", "Oficina de")
    text = text.replace("Foliode", "Folio de")
    text = add_missing_spaces(text)
    return text

def add_missing_spaces(text):
    # Add spaces where needed between words
    return re.sub(r'([a-z])([A-Z])', r'\1 \2', text)

def extract_full_text(pdf_stream):
    """Extract full text from the PDF for debugging purposes."""
    try:
        reader = PdfReader(pdf_stream)
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text()
        return full_text
    except Exception as e:
        logger.error(f"Error extracting full text from DEMANDA: {e}")
        return ""

def pair_pdfs(pdf_info_list):
    """
    Pairs ACUSE and DEMANDA PDFs based on the extracted names.
    """
    paired_pdfs = []
    errors = []

    # Group PDFs by types
    acuse_pdfs = [info for info in pdf_info_list if info['info']['type'] == 'ACUSE']
    demanda_pdfs = [info for info in pdf_info_list if info['info']['type'] == 'DEMANDA']

    # Pair by matching names
    for acuse in acuse_pdfs:
        matched_demandas = [demanda for demanda in demanda_pdfs if demanda['info']['name'] == acuse['info']['name']]

        if matched_demandas:
            paired_pdfs.append({
                'pdfs': [acuse['content'], matched_demandas[0]['content']],
                'client_number': acuse['file_name'].split('_')[0],  # Extract client number from filename
                'name': acuse['info']['name'],
                'info': acuse['info']
            })
            demanda_pdfs.remove(matched_demandas[0])  # Remove matched DEMANDA to prevent duplicate matches
        else:
            errors.append({
                'file_name': acuse['file_name'],
                'message': f"No matching DEMANDA found for ACUSE: {acuse['info']['name']}"
            })

    # Handle unmatched DEMANDA PDFs
    for demanda in demanda_pdfs:
        errors.append({
            'file_name': demanda['file_name'],
            'message': f"No matching ACUSE found for DEMANDA: {demanda['info']['name']}"
        })

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
