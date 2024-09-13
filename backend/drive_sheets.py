from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import pandas as pd
import logging
from backend.utils import normalize_text

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_drive_service(credentials):
    """Get the Google Drive API service."""
    return build('drive', 'v3', credentials=credentials)

def get_sheets_service(credentials):
    """Get the Google Sheets API service."""
    return build('sheets', 'v4', credentials=credentials)

def get_or_create_folder(folder_name, drive_service):
    """Get or create a folder in Google Drive."""
    try:
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        if files:
            logger.info(f"Folder '{folder_name}' already exists in Google Drive.")
            return files[0]['id']
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            logger.info(f"Folder '{folder_name}' created in Google Drive.")
            return folder.get('id')
    except Exception as e:
        logger.error(f"Failed to get or create folder '{folder_name}': {str(e)}")
        raise

def upload_excel_to_drive(file, drive_service):
    """Upload an Excel file to Google Drive and convert it to Google Sheets."""
    try:
        file_metadata = {
            'name': file.filename,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        media = MediaIoBaseUpload(file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"Excel file '{file.filename}' uploaded to Google Drive.")
        return uploaded_file.get('id')
    except Exception as e:
        logger.error(f"Failed to upload Excel file: {str(e)}")
        raise

def upload_file_to_drive(file_stream, folder_id, drive_service, file_name):
    """Upload a file (PDF) to a specific folder in Google Drive."""
    try:
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaIoBaseUpload(file_stream, mimetype='application/pdf')
        uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logger.info(f"PDF '{file_name}' uploaded to folder '{folder_id}' in Google Drive.")
        return uploaded_file.get('id')
    except Exception as e:
        logger.error(f"Failed to upload file '{file_name}' to Google Drive: {str(e)}")
        raise

def read_sheet_data(sheet_id, sheets_service):
    """Read data from a Google Sheets file and ensure at least 19 columns."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:Z1000'  # Fetches up to 26 columns
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.error("No data found in the sheet.")
            return None

        # Log the number of columns found
        header = values[0]
        logger.info(f"Columns in sheet: {len(header)}")
        logger.info(f"Column names: {header}")

        # Handle potential empty columns by padding rows
        max_cols = 19  # Expected number of columns
        for row in values:
            row.extend([''] * (max_cols - len(row)))  # Pad missing columns

        # Create DataFrame with padded rows
        df = pd.DataFrame(values[1:], columns=header + [f'Empty_Col_{i}' for i in range(len(header), max_cols)])
        logger.info(f"Sheet processed with {len(df.columns)} columns.")

        # Ensure the required columns are present
        required_columns = ['Folio de Registro', 'Oficina de Correspondencia']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
                logger.info(f"Added missing column: {col}")

        return df

    except Exception as e:
        logger.error(f"Error reading sheet data: {str(e)}")
        return None

def update_google_sheet(sheet_id, client_number, folio_number, office, sheets_service):
    """Update a Google Sheet with the given client information."""
    # Read existing data
    df = read_sheet_data(sheet_id, sheets_service)
    if df is None:
        logger.error(f"Failed to read data from sheet with ID {sheet_id}.")
        return False

    # Normalize client names
    if 'Client_Name' not in df.columns:
        logger.error("Column 'Client_Name' not found in the sheet.")
        return False

    # Add normalized name column for comparison
    df['Normalized_Name'] = df['Client_Name'].apply(normalize_text)
    client_norm = normalize_text(client_number)

    # Find the row to update
    row_index = df[df['Normalized_Name'] == client_norm].index
    if not row_index.empty:
        logger.info(f"Client '{client_number}' found in the sheet. Updating information...")
        # Update the row with folio_number and office
        df.loc[row_index, 'Folio de Registro'] = folio_number
        df.loc[row_index, 'Oficina de Correspondencia'] = office

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
        logger.info(f"Successfully updated client '{client_number}' in the Google Sheet.")
        return True
    else:
        logger.warning(f"Client '{client_number}' not found in the sheet.")
        return False
