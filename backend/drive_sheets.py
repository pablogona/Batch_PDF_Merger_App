from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import pandas as pd
import logging
from backend.utils import normalize_text
import time

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_drive_service(credentials):
    """Get the Google Drive API service."""
    return build('drive', 'v3', credentials=credentials)

def get_sheets_service(credentials):
    """Get the Google Sheets API service."""
    return build('sheets', 'v4', credentials=credentials)

def get_or_create_folder(folder_name, drive_service, parent_id='root'):
    """Get or create a folder in Google Drive."""
    try:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{parent_id}' in parents and trashed=false"
        response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        if files:
            folder_id = files[0]['id']
            logger.info(f"Folder '{folder_name}' already exists in Google Drive.")
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_id = folder.get('id')
            logger.info(f"Folder '{folder_name}' created in Google Drive.")
        return folder_id
    except Exception as e:
        logger.error(f"Failed to get or create folder '{folder_name}': {str(e)}")
        raise

def upload_excel_to_drive(file_stream, file_name, drive_service, parent_folder_id=None):
    """Upload an Excel file to Google Drive and convert it to Google Sheets."""
    try:
        file_metadata = {
            'name': file_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        media = MediaIoBaseUpload(file_stream, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"Excel file '{file_name}' uploaded to Google Drive.")
        return uploaded_file.get('id')
    except Exception as e:
        logger.error(f"Failed to upload Excel file: {str(e)}")
        raise


def upload_file_to_drive(file_stream, folder_id, drive_service, file_name, retries=3):
    """Upload a file (PDF) to a specific folder in Google Drive, with retry logic."""
    attempt = 0
    while attempt < retries:
        try:
            file_stream.seek(0)  # Ensure stream is at position 0
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            media = MediaIoBaseUpload(file_stream, mimetype='application/pdf')
            uploaded_file = drive_service.files().create(
                body=file_metadata, media_body=media, fields='id'
            ).execute()
            logger.info(f"PDF '{file_name}' uploaded to folder '{folder_id}' in Google Drive.")
            return uploaded_file.get('id')
        except Exception as e:
            attempt += 1
            logger.error(
                f"Failed to upload file '{file_name}' to Google Drive: {str(e)}. Attempt {attempt}/{retries}"
            )
            if attempt < retries:
                logger.info("Retrying upload...")
                time.sleep(2)  # Optional: wait for a short time before retrying
            else:
                raise e


def read_sheet_data(sheet_id, sheets_service):
    """Read data from a Google Sheets file and ensure rows have the same number of columns as the header."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:Z1000'  # Adjust range as needed
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.error("No data found in the sheet.")
            return None

        header = values[0]
        data = values[1:]

        # Ensure each row has the same length as the header
        num_cols = len(header)
        for i, row in enumerate(data):
            if len(row) < num_cols:
                data[i] = row + [''] * (num_cols - len(row))
            elif len(row) > num_cols:
                data[i] = row[:num_cols]

        # Create DataFrame with padded rows
        df = pd.DataFrame(data, columns=header)
        logger.info(f"Sheet processed with {len(df.columns)} columns.")

        # Ensure the required columns are present
        required_columns = ['FOLIO DE REGISTRO', 'OFICINA DE CORRESPONDENCIA']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
                logger.info(f"Added missing column: {col}")

        return df

    except Exception as e:
        logger.error(f"Error reading sheet data: {str(e)}")
        return None

def update_google_sheet(sheet_id, client_name, folio_number, office, sheets_service):
    """Update a Google Sheet with the given client information."""
    # Read existing data
    df = read_sheet_data(sheet_id, sheets_service)
    if df is None:
        logger.error(f"Failed to read data from sheet with ID {sheet_id}.")
        return None

    # Normalize client names
    if 'NOMBRE_CTE' not in df.columns:
        logger.error("Column 'NOMBRE_CTE' not found in the sheet.")
        return None

    # Add normalized name column for comparison
    df['Normalized_Name'] = df['NOMBRE_CTE'].apply(normalize_text)
    client_norm = normalize_text(client_name)

    # Find the row to update
    row_index = df[df['Normalized_Name'] == client_norm].index
    if not row_index.empty:
        logger.info(f"Client '{client_name}' found in the sheet. Updating information...")

        # Extract CLIENTE_UNICO for file naming
        client_unique = df.loc[row_index, 'CLIENTE_UNICO'].values[0]

        # Update the row with folio_number and office
        df.loc[row_index, 'FOLIO DE REGISTRO'] = folio_number
        df.loc[row_index, 'OFICINA DE CORRESPONDENCIA'] = office

        # Remove 'Normalized_Name' column before writing back
        if 'Normalized_Name' in df.columns:
            df = df.drop(columns=['Normalized_Name'])

        # Write updated data back to Google Sheets
        body = {
            'values': [df.columns.tolist()] + df.fillna('').values.tolist()
        }
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(f"Successfully updated client '{client_name}' in the Google Sheet.")

        return client_unique  # Return CLIENTE_UNICO to use for naming the PDF
    else:
        logger.warning(f"Client '{client_name}' not found in the sheet.")
        return None

def get_folder_ids(drive_service, folder_name):
    """Get or create the main folder and timestamped process subfolders, and return their IDs."""
    main_folder_name = 'PDF Merger App'
    subfolders = ['PDFs Unificados', 'PDFs con Error', 'PDFs Originales']

    # Get or create main folder
    main_folder_id = get_or_create_folder(main_folder_name, drive_service)

    # Create timestamped folder for this process
    process_folder_id = get_or_create_folder(folder_name, drive_service, parent_id=main_folder_id)

    folder_ids = {}
    for subfolder in subfolders:
        folder_id = get_or_create_folder(subfolder, drive_service, parent_id=process_folder_id)
        folder_ids[subfolder] = folder_id

    return process_folder_id, folder_ids
