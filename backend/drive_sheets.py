# backend/drive_sheets.py

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import pandas as pd
import logging
from backend.utils import normalize_text
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception
from googleapiclient.errors import HttpError
from threading import Lock

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global cache for sheet data
sheet_cache = {}
sheet_cache_lock = Lock()

def is_retryable_exception(exception):
    """Determine if an exception is retryable based on HTTP status codes."""
    if isinstance(exception, HttpError):
        if exception.resp.status in [500, 502, 503, 504]:
            return True
    return False

# Retry configuration for Google API calls
retry_decorator = retry(
    retry=retry_if_exception(is_retryable_exception),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True
)

def get_drive_service(credentials):
    """Get the Google Drive API service."""
    try:
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Google Drive service initialized.")
        return service
    except Exception as e:
        logger.error(f"Failed to initialize Google Drive service: {str(e)}")
        raise

def get_sheets_service(credentials):
    """Get the Google Sheets API service."""
    try:
        service = build('sheets', 'v4', credentials=credentials)
        logger.info("Google Sheets service initialized.")
        return service
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets service: {str(e)}")
        raise

def get_sheet_names(sheet_id, sheets_service):
    """
    Retrieve the sheet names from the Google Sheets file.
    """
    try:
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        logger.info(f"Sheet names in spreadsheet '{sheet_id}': {sheet_names}")
        return sheet_names
    except Exception as e:
        logger.error(f"Error retrieving sheet names from spreadsheet '{sheet_id}': {e}")
        raise

@retry_decorator
def get_or_create_folder(folder_name, drive_service, parent_id='root'):
    """
    Get or create a folder in Google Drive.

    :param folder_name: Name of the folder to get or create.
    :param drive_service: Authorized Google Drive service instance.
    :param parent_id: ID of the parent folder. Defaults to 'root'.
    :return: Folder ID.
    """
    try:
        query = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and '{parent_id}' in parents and trashed=false"
        )
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=10
        ).execute()
        files = response.get('files', [])
        if files:
            folder_id = files[0]['id']
            logger.info(f"Folder '{folder_name}' already exists in Google Drive with ID: {folder_id}")
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            folder = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_id = folder.get('id')
            logger.info(f"Folder '{folder_name}' created in Google Drive with ID: {folder_id}")
        return folder_id
    except HttpError as e:
        logger.error(f"HttpError in get_or_create_folder for '{folder_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error in get_or_create_folder for '{folder_name}': {e}")
        raise

@retry_decorator
def upload_excel_to_drive(file_stream, file_name, drive_service, parent_folder_id=None):
    """
    Upload an Excel file to Google Drive and convert it to Google Sheets.

    :param file_stream: File stream of the Excel file.
    :param file_name: Name of the Excel file.
    :param drive_service: Authorized Google Drive service instance.
    :param parent_folder_id: ID of the parent folder in Drive.
    :return: Spreadsheet ID of the uploaded Google Sheet.
    """
    try:
        file_metadata = {
            'name': file_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        media = MediaIoBaseUpload(
            file_stream,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        uploaded_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        spreadsheet_id = uploaded_file.get('id')
        logger.info(f"Excel file '{file_name}' uploaded to Google Drive as Google Sheet with ID: {spreadsheet_id}")
        return spreadsheet_id
    except HttpError as e:
        logger.error(f"HttpError in upload_excel_to_drive for '{file_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error in upload_excel_to_drive for '{file_name}': {e}")
        raise

@retry_decorator
def upload_file_to_drive(file_stream, folder_id, drive_service, file_name, mimetype='application/pdf'):
    """
    Upload a file to a specific folder in Google Drive.

    :param file_stream: File stream of the file.
    :param folder_id: ID of the destination folder in Drive.
    :param drive_service: Authorized Google Drive service instance.
    :param file_name: Name of the file.
    :param mimetype: MIME type of the file.
    :return: Uploaded file ID.
    """
    try:
        file_stream.seek(0)  # Ensure stream is at position 0
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        media = MediaIoBaseUpload(
            file_stream,
            mimetype=mimetype,
            resumable=True
        )
        uploaded_file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        file_id = uploaded_file.get('id')
        logger.info(f"File '{file_name}' uploaded to folder '{folder_id}' in Google Drive with ID: {file_id}")
        return file_id
    except HttpError as e:
        logger.error(f"HttpError in upload_file_to_drive for '{file_name}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error in upload_file_to_drive for '{file_name}': {e}")
        raise

@retry_decorator
def read_sheet_data(sheet_id, sheets_service):
    """
    Read data from a Google Sheets file and ensure rows have the same number of columns as the header.
    If missing columns are added, update the Google Sheet to reflect these changes.
    Caches the data to minimize API calls.

    :param sheet_id: ID of the Google Sheet.
    :param sheets_service: Authorized Google Sheets service instance.
    :return: Tuple of pandas DataFrame containing the sheet data and the sheet name.
    """
    try:
        with sheet_cache_lock:
            if sheet_id in sheet_cache:
                logger.info(f"Using cached data for sheet '{sheet_id}'")
                return sheet_cache[sheet_id], sheet_cache[f"{sheet_id}_sheet_name"]

        # Get the sheet names
        sheet_names = get_sheet_names(sheet_id, sheets_service)
        if not sheet_names:
            logger.error(f"No sheets found in spreadsheet '{sheet_id}'.")
            return None, None

        # Use the first sheet name
        sheet_name = sheet_names[0]
        logger.info(f"Using sheet '{sheet_name}' in spreadsheet '{sheet_id}'.")

        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{sheet_name}'!A1:Z"  # Adjust range as needed
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.error("No data found in the sheet.")
            return None, None

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
        logger.info(f"Sheet '{sheet_name}' processed with {len(df.columns)} columns.")

        # Ensure the required columns are present
        required_columns = ['FOLIO DE REGISTRO', 'OFICINA DE CORRESPONDENCIA']
        new_columns_added = False
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
                new_columns_added = True
                logger.info(f"Added missing column: {col}")

        if new_columns_added:
            # Update the sheet with new columns
            update_sheet_with_new_columns(sheet_id, sheet_name, df.columns.tolist(), sheets_service)
            logger.info(f"Google Sheet '{sheet_id}' updated with new columns.")

        with sheet_cache_lock:
            sheet_cache[sheet_id] = df  # Cache the DataFrame
            sheet_cache[f"{sheet_id}_sheet_name"] = sheet_name  # Cache the sheet name

        return df, sheet_name

    except HttpError as e:
        logger.error(f"HttpError in read_sheet_data for sheet '{sheet_id}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error in read_sheet_data for sheet '{sheet_id}': {e}")
        raise

def update_sheet_with_new_columns(sheet_id, sheet_name, columns, sheets_service):
    """
    Update the Google Sheet with new columns added to the DataFrame.

    :param sheet_id: ID of the Google Sheet.
    :param sheet_name: Name of the sheet to update.
    :param columns: List of column names.
    :param sheets_service: Authorized Google Sheets service instance.
    """
    try:
        # Prepare the header row with the updated columns
        body = {
            'values': [columns]
        }
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(f"Sheet '{sheet_name}' header updated with new columns.")
    except Exception as e:
        logger.error(f"Error updating sheet '{sheet_name}' with new columns: {e}")
        raise

def col_idx_to_letter(idx):
    """Convert a zero-based column index to a column letter."""
    idx += 1  # Convert to 1-based index
    letters = ''
    while idx:
        idx, remainder = divmod(idx - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

def update_google_sheet(sheet_id, client_name, folio_number, office, sheets_service, batch_updates=None):
    """
    Update a Google Sheet with the given client information.
    Supports both single and batch updates.

    :param sheet_id: ID of the Google Sheet.
    :param client_name: Name of the client to update.
    :param folio_number: Folio number to set.
    :param office: Office to set.
    :param sheets_service: Authorized Google Sheets service instance.
    :param batch_updates: List to collect batch update data. If provided, the update will be added to this list.
    :return: CLIENTE_UNICO of the updated client or None.
    """
    try:
        # Read existing data and get the sheet name
        df, sheet_name = read_sheet_data(sheet_id, sheets_service)
        if df is None or sheet_name is None:
            logger.error(f"Failed to read data from sheet with ID {sheet_id}.")
            return None

        # Normalize client names
        if 'NOMBRE_CTE' not in df.columns:
            logger.error("Column 'NOMBRE_CTE' not found in the sheet.")
            return None

        # Map column names to indices and normalize case for comparison
        column_indices = {col_name: idx for idx, col_name in enumerate(df.columns)}

        # Find the index of the 'Folio' column by checking if "Folio" is in the column name, case-insensitive
        folio_col_idx = next((idx for col_name, idx in column_indices.items() if 'folio' in col_name.lower()), None)

        # Find the index of the 'Oficina' column by checking if "Oficina" is in the column name, case-insensitive
        office_col_idx = next((idx for col_name, idx in column_indices.items() if 'oficina' in col_name.lower()), None)

        # Find the index of the 'CLIENTE_UNICO' column (if present)
        client_unique_col_idx = column_indices.get('CLIENTE_UNICO')

        # Add normalized name column for comparison
        df['Normalized_Name'] = df['NOMBRE_CTE'].apply(normalize_text)
        client_norm = normalize_text(client_name)

        # Find the row to update
        row_index = df[df['Normalized_Name'] == client_norm].index
        if not row_index.empty:
            row_number = row_index[0] + 2  # Data starts from row 2 if header is at row 1
            logger.info(f"Client '{client_name}' found in the sheet at row {row_number}. Preparing to update.")

            # Extract CLIENTE_UNICO for file naming
            if client_unique_col_idx is not None:
                client_unique = df.iloc[row_index[0], client_unique_col_idx]
            else:
                logger.warning("Column 'CLIENTE_UNICO' not found in the sheet. Skipping CLIENTE_UNICO extraction.")
                client_unique = ''

            updates = []

            # Prepare updates for each column individually
            # Update 'FOLIO' (whatever it's called)
            folio_col_letter = col_idx_to_letter(folio_col_idx)
            folio_range = f"'{sheet_name}'!{folio_col_letter}{row_number}"
            folio_values = [[folio_number]]
            updates.append({'range': folio_range, 'values': folio_values})

            # Update 'OFICINA' (whatever it's called)
            office_col_letter = col_idx_to_letter(office_col_idx)
            office_range = f"'{sheet_name}'!{office_col_letter}{row_number}"
            office_values = [[office]]
            updates.append({'range': office_range, 'values': office_values})

            if batch_updates is not None:
                # Add updates to batch_updates
                for update in updates:
                    batch_updates.append(update)
                    logger.info(f"Added update for client '{client_name}': Range: {update['range']}, Values: {update['values']}")
            else:
                # Perform updates individually
                for update in updates:
                    body = {
                        'values': update['values']
                    }
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=update['range'],
                        valueInputOption='RAW',
                        body=body
                    ).execute()
                    logger.info(f"Successfully updated range '{update['range']}' for client '{client_name}'.")

            return client_unique  # Return CLIENTE_UNICO to use for naming the PDF
        else:
            logger.warning(f"Client '{client_name}' not found in the sheet.")
            return None

    except Exception as e:
        logger.error(f"Error in update_google_sheet for client '{client_name}': {e}")
        raise

@retry_decorator
def batch_update_google_sheet(spreadsheet_id, data, sheets_service):
    """
    Batch update multiple ranges in the Google Sheet.

    :param spreadsheet_id: ID of the spreadsheet to update.
    :param data: List of dictionaries with 'range' and 'values'.
    :param sheets_service: Authorized Sheets API service instance.
    :return: Result of the batch update.
    """
    try:
        # Split data into chunks to avoid exceeding API limits
        chunk_size = 100  # Adjust the chunk size as needed
        total_updates = len(data)
        logger.info(f"Total updates to perform: {total_updates}")

        for i in range(0, total_updates, chunk_size):
            chunk = data[i:i + chunk_size]
            body = {
                'valueInputOption': 'RAW',
                'data': chunk
            }
            logger.info(f"Performing batch update for records {i + 1} to {i + len(chunk)}")
            sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            logger.info(f"Batch updated Google Sheet '{spreadsheet_id}' with {len(chunk)} updates.")
        return True
    except HttpError as e:
        logger.error(f"HttpError in batch_update_google_sheet for sheet '{spreadsheet_id}': {e}")
        raise
    except Exception as e:
        logger.error(f"Error in batch_update_google_sheet for sheet '{spreadsheet_id}': {e}")
        raise

def get_folder_ids(drive_service, folder_name):
    """
    Get or create the main folder and timestamped process subfolders, and return their IDs.

    :param drive_service: Authorized Google Drive service instance.
    :param folder_name: Name of the timestamped process folder.
    :return: Tuple containing the process folder ID and a dictionary of subfolder IDs.
    """
    try:
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
            logger.info(f"Subfolder '{subfolder}' has ID: {folder_id}")

        return process_folder_id, folder_ids
    except Exception as e:
        logger.error(f"Error in get_folder_ids for '{folder_name}': {e}")
        raise
