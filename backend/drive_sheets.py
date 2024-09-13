from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import pandas as pd
import logging
from backend.utils import normalize_text

def get_drive_service(credentials):
    return build('drive', 'v3', credentials=credentials)

def get_sheets_service(credentials):
    return build('sheets', 'v4', credentials=credentials)

def get_or_create_folder(folder_name, drive_service):
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])
    if files:
        return files[0]['id']
    else:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = drive_service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

def upload_excel_to_drive(file, drive_service):
    file_metadata = {
        'name': file.filename,
        'mimeType': 'application/vnd.google-apps.spreadsheet'
    }
    media = MediaIoBaseUpload(file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return uploaded_file.get('id')

def upload_file_to_drive(file_stream, folder_id, drive_service, file_name):
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaIoBaseUpload(file_stream, mimetype='application/pdf')
    uploaded_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return uploaded_file.get('id')

def read_sheet_data(sheet_id, sheets_service):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range='A1:Z1000'
        ).execute()
        values = result.get('values', [])
        if not values:
            logging.error("No data found in the sheet.")
            return None
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    except Exception as e:
        logging.error(f"Error reading sheet data: {str(e)}")
        return None

def update_google_sheet(sheet_id, client_number, folio_number, office, sheets_service):
    # Read existing data
    df = read_sheet_data(sheet_id, sheets_service)
    if df is None:
        return False

    # Normalize client names
    if 'Client_Name' not in df.columns:
        logging.error("Column 'Client_Name' not found in sheet.")
        return False

    df['Normalized_Name'] = df['Client_Name'].apply(normalize_text)
    client_norm = normalize_text(client_number)

    # Find row to update
    row_index = df[df['Normalized_Name'] == client_norm].index
    if not row_index.empty:
        # Add columns if they don't exist
        if 'Folio de Registro' not in df.columns:
            df['Folio de Registro'] = ''
        if 'Oficina de Correspondencia' not in df.columns:
            df['Oficina de Correspondencia'] = ''

        # Update the row with folio_number and office
        df.loc[row_index, 'Folio de Registro'] = folio_number
        df.loc[row_index, 'Oficina de Correspondencia'] = office

        # Write back to Google Sheets
        body = {
            'values': [df.columns.tolist()] + df.fillna('').values.tolist()
        }
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        return True
    else:
        logging.warning(f"Client number {client_number} not found in sheet.")
        return False
