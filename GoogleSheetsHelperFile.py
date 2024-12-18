from googleapiclient import discovery
from google.oauth2 import service_account
import json


def authenticate_google_creds(
        service_account_file: str,
        session_scopes) -> service_account.Credentials:
    """Authenticates service account credentials and returns ServiceAccountCredentials object"""
    SERVICE_ACCOUNT_FILE = json.loads(service_account_file)
    SCOPES = session_scopes

    creds = None
    creds = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return creds


def get_google_sheet(
        service_account_file: str,
        spreadsheet_id: str,
        spreadsheet_range: str) -> dict:
    """Establish Google Sheets API session & read data from a sheet range.
    Returns a dictionary with API result."""
    service_account_creds = authenticate_google_creds(
        service_account_file=service_account_file,
        session_scopes=['https://www.googleapis.com/auth/spreadsheets']
    )

    service = discovery.build('sheets', 'v4', credentials=service_account_creds)

    # Call the Sheets API & get values from spreadsheet
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=spreadsheet_range).execute()
    return result


def drive_file_modified_time(
        service_account_file: str,
        file_id: str) -> dict:
    service_account_creds = authenticate_google_creds(
        service_account_file=service_account_file,
        session_scopes=['https://www.googleapis.com/auth/drive.metadata.readonly']
    )
    service = discovery.build("drive", "v3", credentials=service_account_creds)
    fileId = file_id
    file_modifiedTime_response = service.files().get(fileId=fileId, fields="modifiedTime").execute()
    return file_modifiedTime_response

