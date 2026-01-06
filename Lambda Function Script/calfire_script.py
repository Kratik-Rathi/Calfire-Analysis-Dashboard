from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import requests
import os

# =====================
# CONFIG
# =====================
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "credentials.json")
SPREADSHEET_ID = "Spread Sheet ID"
SHEET_NAME = "Sheet Name"
BASE_API_URL = "Cal-fire API URL"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

ID_COL = "incident_id"
UPD_COL = "incident_date_last_update"

# Persistent HTTP session
SESSION = requests.Session()


# =====================
# AUTH + SERVICE
# =====================
def build_sheets_handles():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=creds)
    spreadsheets = service.spreadsheets()       # used for .get() and .batchUpdate()
    values = spreadsheets.values()              # used for values().get/append/batchUpdate
    return spreadsheets, values


def get_sheet_id(spreadsheets, spreadsheet_id: str, sheet_name: str) -> int:
    """Resolve sheetId by title using the Spreadsheets resource (NOT values)."""
    meta = spreadsheets.get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return int(props.get("sheetId"))
    raise ValueError(f"Sheet named '{sheet_name}' not found.")


# =====================
# API LOAD
# =====================
def get_api_data() -> pd.DataFrame:
    current_year = datetime.datetime.now().year

    def fetch(year: int):
        """Fetch raw JSON for a given year, including inactive incidents."""
        url = f"{BASE_API_URL}?year={year}&inactive=true"
        print(f"Fetching CAL FIRE data from: {url}")
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # Try current year
    try:
        data = fetch(current_year)
    except Exception as e:
        # This is a real error (network, 5xx, bad JSON) → let it bubble up
        # so your __main__ / lambda_handler can return 500.
        print(f"Error fetching data for year {current_year}: {e}")
        raise

    features = data.get("features", []) if isinstance(data, dict) else []
    if not features:
        print(f"No incidents found for {current_year}, trying {current_year - 1}...")
        try:
            data = fetch(current_year - 1)
            features = data.get("features", []) if isinstance(data, dict) else []
        except Exception as e:
            print(f"Error fetching data for year {current_year - 1}: {e}")
            # Propagate, same logic: this is a real failure, not "no data"
            raise

        if not features:
            print("No incidents found for current or previous year. Returning empty DataFrame.")
            return pd.DataFrame()

    # We have some features → normalize to DataFrame
    records = [f.get("properties", {}) for f in features]
    df = pd.json_normalize(records)

    df = df.rename(
        columns={
            "Name": "incident_name",
            "Final": "incident_is_final",
            "Updated": "incident_date_last_update",
            "Started": "incident_date_created",
            "AdminUnit": "incident_administrative_unit",
            "AdminUnitUrl": "incident_administrative_unit_url",
            "County": "incident_county",
            "Location": "incident_location",
            "AcresBurned": "incident_acres_burned",
            "PercentContained": "incident_containment",
            "ControlStatement": "incident_control_statement",
            "AgencyNames": "incident_agency_names",
            "Longitude": "incident_longitude",
            "Latitude": "incident_latitude",
            "Type": "incident_type",
            "UniqueId": "incident_id",
            "Url": "incident_url",
            "ExtinguishedDate": "incident_date_extinguished",
            "ExtinguishedDateOnly": "incident_dateonly_extinguished",
            "StartedDateOnly": "incident_dateonly_created",
            "IsActive": "is_active",
            "CalFireIncident": "calfire_incident",
            "NotificationDesired": "notification_desired",
        }
    )

    return df

# =====================
# HELPERS
# =====================
def normalize_iso(ts):
    """Normalize timestamps to ISO-8601 strings; pass through blanks safely."""
    if ts is None or (isinstance(ts, str) and ts.strip() == ""):
        return ""
    try:
        return pd.to_datetime(ts).isoformat()
    except Exception:
        return str(ts)


def read_sheet_as_df(values, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    res = values.get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    rows = res.get("values", [])
    if not rows:
        raise ValueError("Sheet is empty or headers missing.")

    headers = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=headers).fillna("")

    # Ensure all headers exist (ragged protection) and keep order
    for col in headers:
        if col not in df.columns:
            df[col] = ""
    df = df[headers]
    return df


def sort_sheet(spreadsheets, spreadsheet_id: str, sheet_name: str, headers: list, sort_col_name: str):
    """Always attempt to sort (even if no appends/updates occurred)."""
    sheet_id = get_sheet_id(spreadsheets, spreadsheet_id, sheet_name)
    if sort_col_name not in headers:
        print(f"Sort skipped: '{sort_col_name}' not in headers.")
        return
    sort_index = headers.index(sort_col_name)

    sort_request = {
        "requests": [{
            "sortRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,     # skip header
                    "startColumnIndex": 0
                    # end indices omitted => to the end
                },
                "sortSpecs": [{
                    "dimensionIndex": sort_index,
                    "sortOrder": "DESCENDING"
                }]
            }
        }]
    }
    spreadsheets.batchUpdate(spreadsheetId=spreadsheet_id, body=sort_request).execute()
    print(f"Sorted by '{sort_col_name}' (DESC).")


# =====================
# CORE SYNC
# =====================
def sync_to_sheet():
    spreadsheets, values = build_sheets_handles()

    # 1) Read existing sheet
    try:
        sheet_df = read_sheet_as_df(values, SPREADSHEET_ID, SHEET_NAME)
    except ValueError as e:
        # If sheet is empty, we cannot diff/sort. Just load API data and write headers+rows.
        print(str(e))
        df_api = get_api_data()
        if df_api.empty:
            print("API returned no data; nothing to write.")
            return

        headers = list(df_api.columns)
        # Normalize update column if present
        if UPD_COL in df_api.columns:
            df_api[UPD_COL] = df_api[UPD_COL].map(normalize_iso)

        body = {
            "range": SHEET_NAME,
            "values": [headers] + df_api.astype(str).values.tolist()
        }
        values.update(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        # Try to sort after initial write
        try:
            sort_sheet(spreadsheets, SPREADSHEET_ID, SHEET_NAME, headers, UPD_COL)
        except Exception as se:
            print(f"Sort skipped: {se}")
        return

    headers = list(sheet_df.columns)

    # Ensure ID/UPD columns exist
    if ID_COL not in headers:
        raise ValueError(f"Required header '{ID_COL}' not found in the sheet.")
    if UPD_COL not in headers:
        raise ValueError(f"Required header '{UPD_COL}' not found in the sheet.")

    # Normalize existing sheet update timestamps
    if UPD_COL in sheet_df.columns:
        sheet_df[UPD_COL] = sheet_df[UPD_COL].map(normalize_iso)

    # Build map: incident_id -> (2-based sheet row number, old_update_iso)
    existing_map = {}
    if not sheet_df.empty:
        for i, r in sheet_df.iterrows():
            rid = str(r.get(ID_COL, "")).strip()
            if rid:
                existing_map[rid] = (i + 2, r.get(UPD_COL, ""))

    # 2) Load API data and align columns
    df = get_api_data()
    df = df.reindex(columns=headers, fill_value="")   # keep only known headers; add missing as empty

    # Normalize API update timestamps for fair comparison
    if UPD_COL in df.columns:
        df[UPD_COL] = df[UPD_COL].map(normalize_iso)

    # 3) Classify vectorized: new vs needs update
    df[ID_COL] = df[ID_COL].astype(str)
    df["_exists"] = df[ID_COL].isin(existing_map.keys())

    old_updates = df[ID_COL].map({k: v[1] for k, v in existing_map.items()})
    needs_update_mask = df["_exists"] & (df[UPD_COL] != old_updates.fillna(""))

    to_append_df = df.loc[~df["_exists"], headers]
    to_update_df = df.loc[needs_update_mask, headers]

    # 4) Append new rows (single call)
    to_append = to_append_df.astype(str).values.tolist()
    if to_append:
        values.append(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": to_append},
        ).execute()
        print(f"Appended {len(to_append)} new rows.")

    # 5) Batch update changed rows (single call)
    if not to_update_df.empty:
        update_data = []
        for rid, row_vals in zip(
            to_update_df[ID_COL].tolist(),
            to_update_df.astype(str).values.tolist(),
        ):
            row_num = existing_map[rid][0]  # already 2-based
            update_data.append({"range": f"{SHEET_NAME}!A{row_num}", "values": [row_vals]})

        values.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": update_data},
        ).execute()
        print(f"Updated {len(update_data)} existing rows.")

    # 6) ALWAYS sort (even if nothing changed)
    try:
        sort_sheet(spreadsheets, SPREADSHEET_ID, SHEET_NAME, headers, UPD_COL)
    except HttpError as he:
        print(f"Sort failed (non-fatal): {he}")
    except Exception as e:
        print(f"Sort skipped: {e}")


# =====================
# ENTRY POINTS
# =====================
if __name__ == "__main__":
    try:
        sync_to_sheet()
    except Exception as e:
        print(f"Error: {e}")


def lambda_handler(event=None, context=None):
    try:
        sync_to_sheet()
        return {"statusCode": 200, "body": "Sync completed successfully."}
    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
