import os
import pandas as pd
from datetime import datetime
import warnings
import shutil
import tempfile
import re
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename

warnings.filterwarnings('ignore')

# --- Vercel Specific Path Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(BASE_DIR, '..', 'templates')
static_dir = os.path.join(BASE_DIR, '..', 'static')

# Initialize Flask app
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_secret_key_for_local_dev_only')

# --- Global Variables ---
CONSOLIDATED_OUTPUT_COLUMNS = [
    'Barcode', 'Processor', 'Channel', 'Category', 'Company code', 'Region',
    'Vendor number', 'Vendor Name', 'Status', 'Received Date', 'Re-Open Date',
    'Allocation Date', 'Clarification Date', 'Completion Date', 'Requester',
    'Remarks', 'Aging', 'Today'
]

# --- Helper Functions ---

def format_date_to_mdyyyy(date_series):
    """
    Formats a pandas Series of dates to MM/DD/YYYY string format.
    Handles potential mixed types and NaT values.
    """
    datetime_series = pd.to_datetime(date_series, errors='coerce')
    formatted_series = datetime_series.apply(
        lambda x: f"{x.month}/{x.day}/{x.year}" if pd.notna(x) else ''
    )
    return formatted_series

def clean_column_names(df):
    """
    Cleans DataFrame column names by:
    1. Lowercasing all characters.
    2. Replacing spaces with underscores.
    3. Removing special characters (keeping only alphanumeric and underscores).
    4. Removing leading/trailing underscores.
    """
    new_columns = []
    for col in df.columns:
        col = str(col).strip().lower()
        col = re.sub(r'\s+', '_', col)
        col = re.sub(r'[^a-z0-9_]', '', col)
        col = col.strip('_')
        new_columns.append(col)
    df.columns = new_columns
    return df

def calculate_aging(df):
    """
    Calculates the 'Aging' for each row based on 'Received Date' and 'Today'.
    Handles various date formats and NaN values gracefully.
    'Today' is expected to be a datetime object.
    """
    if 'Received Date' in df.columns and 'Today' in df.columns:
        # Convert 'Received Date' to datetime objects, coercing errors
        df['Received_Date_dt'] = pd.to_datetime(df['Received Date'], errors='coerce')
        # Ensure 'Today' is a datetime object or can be converted
        df['Today_dt'] = pd.to_datetime(df['Today'], errors='coerce')

        # Calculate aging only where both dates are valid
        valid_dates_mask = df['Received_Date_dt'].notna() & df['Today_dt'].notna()
        df.loc[valid_dates_mask, 'Aging'] = (df.loc[valid_dates_mask, 'Today_dt'] - df.loc[valid_dates_mask, 'Received_Date_dt']).dt.days
        df['Aging'] = df['Aging'].fillna('').astype(str) # Fill NaN with empty string and convert to str
        df = df.drop(columns=['Received_Date_dt', 'Today_dt'])
    else:
        df['Aging'] = '' # If columns are missing, set Aging to empty string
    return df

def consolidate_data_process(df_pisa, df_esm, df_pm7, df_smd, consolidated_output_file_path):
    """
    Reads PISA, ESM, PM7, and SMD Excel files (now passed as DFs), filters PISA, consolidates data,
    and saves it to a new Excel file.
    """
    print("Starting data consolidation process...")
    print("All input DataFrames loaded successfully!")

    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())
    df_smd = clean_column_names(df_smd.copy()) # Clean SMD column names

    allowed_pisa_users = ["Goswami Sonali", "Patil Jayapal Gowd", "Ranganath Chilamakuri","Sridhar Divya","Sunitha S","Varunkumar N"]
    if 'assigned_user' in df_pisa.columns:
        original_pisa_count = len(df_pisa)
        df_pisa_filtered = df_pisa[df_pisa['assigned_user'].isin(allowed_pisa_users)].copy()
        print(f"\nPISA file filtered. Original records: {original_pisa_count}, Records after filter: {len(df_pisa_filtered)}")
    else:
        print("\nWarning: 'assigned_user' column not found in PISA file (after cleaning). No filter applied.")
        df_pisa_filtered = df_pisa.copy()

    all_consolidated_rows = []
    today_date = datetime.now()

    # --- PISA Processing ---
    if 'barcode' not in df_pisa_filtered.columns:
        print("Error: 'barcode' column not found in PISA file (after cleaning). Skipping PISA processing.")
    else:
        df_pisa_filtered['barcode'] = df_pisa_filtered['barcode'].astype(str)
        for index, row in df_pisa_filtered.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Company code': row.get('company_code'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Completion Date': None, 'Status': None , 'Today': today_date, 'Channel': 'PISA',
                'Vendor Name': row.get('vendor_name'),
                'Re-Open Date': None, 'Allocation Date': None,
                'Requester': None, 'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None,
                'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_pisa_filtered)} rows from PISA.")

    # --- ESM Processing ---
    if 'barcode' not in df_esm.columns:
        print("Error: 'barcode' column not found in ESM file (after cleaning). Skipping ESM processing.")
    else:
        df_esm['barcode'] = df_esm['barcode'].astype(str)
        for index, row in df_esm.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Received Date': row.get('received_date'),
                'Status': row.get('state'),
                'Requester': row.get('opened_by'),
                'Completion Date': row.get('closed') if pd.notna(row.get('closed')) else None,
                'Re-Open Date': row.get('updated') if (row.get('state') or '').lower() == 'reopened' else None,
                'Today': today_date, 'Remarks': row.get('short_description'),
                'Channel': 'ESM',
                'Company code': None,'Vendor Name': None,
                'Vendor number': None, 'Allocation Date': None,
                'Clarification Date': None, 'Aging': None,
                'Region': None,
                'Processor': None,
                'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_esm)} rows from ESM.")

    # --- PM7 Processing ---
    if 'barcode' not in df_pm7.columns:
        print("Error: 'barcode' column not found in PM7 file (after cleaning). Skipping PM7 processing.")
    else:
        df_pm7['barcode'] = df_pm7['barcode'].astype(str)

        for index, row in df_pm7.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Vendor Name': row.get('vendor_name'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Status': row.get('task'),
                'Today': today_date,
                'Channel': 'PM7',
                'Company code': row.get('company_code'),
                'Re-Open Date': None,
                'Allocation Date': None, 'Completion Date': None, 'Requester': None,
                'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None,
                'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_pm7)} rows from PM7.")

    # --- SMD Processing --- ADDED
    if 'barcode' not in df_smd.columns: # Assuming 'barcode' is the unique identifier in SMD
        print("Error: 'barcode' column not found in SMD file (after cleaning). Skipping SMD processing.")
    else:
        df_smd['barcode'] = df_smd['barcode'].astype(str)
        for index, row in df_smd.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Company code': row.get('ekorg'),
                'Region': row.get('material_field'),
                'Vendor number': row.get('pmd_sno'),
                'Vendor Name': row.get('supplier_name'),
                'Received Date': row.get('request_date'),
                'Requester': row.get('requested_by'),
                'Today': today_date,
                'Channel': 'SMD', # Set Channel to SMD
                'Status': None, 'Completion Date': None, # Default None/empty for now
                'Re-Open Date': None, 'Allocation Date': None,
                'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_smd)} rows from SMD.")


    if not all_consolidated_rows:
        return False, "No data collected for consolidation."

    df_consolidated = pd.DataFrame(all_consolidated_rows)

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = None

    df_consolidated = df_consolidated[CONSOLIDATED_OUTPUT_COLUMNS]

    # --- Calculate Aging --- ADDED
    df_consolidated = calculate_aging(df_consolidated)

    date_cols_to_process = ['Received Date', 'Re-Open Date', 'Allocation Date', 'Completion Date', 'Clarification Date', 'Today']
    for col in df_consolidated.columns:
        if col in date_cols_to_process:
            df_consolidated[col] = format_date_to_mdyyyy(df_consolidated[col])
        else:
            if df_consolidated[col].dtype == 'object':
                df_consolidated[col] = df_consolidated[col].fillna('')
            elif col in ['Barcode', 'Company code', 'Vendor number', 'Aging']: # Added Aging here
                df_consolidated[col] = df_consolidated[col].astype(str).replace('nan', '')

    try:
        df_consolidated.to_excel(consolidated_output_file_path, index=False)
        print(f"Consolidated file saved to: {consolidated_output_file_path}")
    except Exception as e:
        return False, f"Error saving consolidated file: {e}"
    print("--- Consolidated Data Process Complete ---")
    return True, df_consolidated

def process_central_file_step2_update_existing(consolidated_df, central_file_input_path):
    """
    Step 2: Updates status of *existing* central file records based on consolidated data.
    """
    print(f"\n--- Starting Central File Status Processing (Step 2: Update Existing Barcodes) ---")

    try:
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())

        print("Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading Consolidated (DF) or Central (file) for processing (Step 2): {e}"

    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with central file processing (Step 2)."
    if 'barcode' not in df_central_cleaned.columns or 'status' not in df_central_cleaned.columns:
        return False, "Error: 'barcode' or 'status' column not found in the central file after cleaning. Cannot update status (Step 2)."

    consolidated_df['Barcode'] = consolidated_df['Barcode'].astype(str)
    df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str)

    df_central_cleaned['Barcode_compare'] = df_central_cleaned['barcode']

    consolidated_barcodes_set = set(consolidated_df['Barcode'].unique())
    print(f"Found {len(consolidated_barcodes_set)} unique barcodes in the consolidated file for Step 2.")

    def transform_status_if_barcode_exists(row):
        central_barcode = str(row['Barcode_compare'])
        original_central_status = row['status']

        if central_barcode in consolidated_barcodes_set:
            if pd.isna(original_central_status) or \
               (isinstance(original_central_status, str) and original_central_status.strip().lower() in ['', 'n/a', 'na', 'none']):
                return original_central_status

            status_str = str(original_central_status).strip().lower()
            if status_str == 'new':
                return 'Untouched'
            elif status_str == 'completed':
                return 'Reopen'
            elif status_str == 'n/a':
                return 'New'
            else:
                return original_central_status
        else:
            return original_central_status

    df_central_cleaned['status'] = df_central_cleaned.apply(transform_status_if_barcode_exists, axis=1)
    df_central_cleaned = df_central_cleaned.drop(columns=['Barcode_compare'])

    print(f"Updated 'status' column in central file for Step 2 for {len(df_central_cleaned)} records.")

    try:
        common_cols_map = {
            'barcode': 'Barcode', 'channel': 'Channel', 'company_code': 'Company code',
            'vendor_name': 'Vendor Name', 'vendor_number': 'Vendor number',
            'received_date': 'Received Date', 're_open_date': 'Re-Open Date',
            'allocation_date': 'Allocation Date', 'completion_date': 'Completion Date',
            'requester': 'Requester', 'clarification_date': 'Clarification Date',
            'aging': 'Aging', 'today': 'Today', 'status': 'Status', 'remarks': 'Remarks',
            'region': 'Region', 'processor': 'Processor', 'category': 'Category'
        }

        cols_to_rename = {k: v for k, v in common_cols_map.items() if k in df_central_cleaned.columns}
        df_central_cleaned.rename(columns=cols_to_rename, inplace=True)

        date_cols_in_central_file = [
            'Received Date', 'Re-Open Date', 'Allocation Date',
            'Completion Date', 'Clarification Date', 'Today'
        ]
        for col in df_central_cleaned.columns:
            if col in date_cols_in_central_file:
                df_central_cleaned[col] = format_date_to_mdyyyy(df_central_cleaned[col])
            elif df_central_cleaned[col].dtype == 'object':
                df_central_cleaned[col] = df_central_cleaned[col].fillna('')
            elif col in ['Barcode', 'Vendor number', 'Aging']: # Added Aging here
                df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')
            if col == 'Company code':
                 df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')

        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_central_cleaned.columns:
                df_central_cleaned[col] = None

    except Exception as e:
        return False, f"Error processing central file (Step 2): {e}"
    print(f"--- Central File Status Processing (Step 2) Complete ---")
    return True, df_central_cleaned


def process_central_file_step3_final_merge_and_needs_review(consolidated_df, updated_existing_central_df, final_central_output_file_path, df_pisa_original, df_esm_original, df_pm7_original, df_smd_original, region_mapping_df): # Added df_smd_original
    """
    Step 3: Handles barcodes present only in consolidated (adds them as new)
            and barcodes present only in central (marks them as 'Needs Review' if not 'Completed').
            Also performs region mapping and final column reordering.
    """
    print(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review) ---")

    df_pisa_lookup = clean_column_names(df_pisa_original.copy())
    df_esm_lookup = clean_column_names(df_esm_original.copy())
    df_pm7_lookup = clean_column_names(df_pm7_original.copy())
    df_smd_lookup = clean_column_names(df_smd_original.copy()) # Added SMD lookup

    df_pisa_indexed = pd.DataFrame()
    if 'barcode' in df_pisa_lookup.columns:
        df_pisa_lookup['barcode'] = df_pisa_lookup['barcode'].astype(str)
        df_pisa_indexed = df_pisa_lookup.set_index('barcode')
        print(f"PISA lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned PISA lookup. Cannot perform PISA lookups.")

    df_esm_indexed = pd.DataFrame()
    if 'barcode' in df_esm_lookup.columns:
        df_esm_lookup['barcode'] = df_esm_lookup['barcode'].astype(str)
        df_esm_indexed = df_esm_lookup.set_index('barcode')
        print(f"ESM lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned ESM lookup. Cannot perform ESM lookups.")

    df_pm7_indexed = pd.DataFrame()
    if 'barcode' in df_pm7_lookup.columns:
        df_pm7_lookup['barcode'] = df_pm7_lookup['barcode'].astype(str)
        df_pm7_indexed = df_pm7_lookup.set_index('barcode')
        print(f"PM7 lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned PM7 lookup. Cannot perform PM7 lookups.")

    df_smd_indexed = pd.DataFrame() # Added SMD lookup
    if 'barcode' in df_smd_lookup.columns:
        df_smd_lookup['barcode'] = df_smd_lookup['barcode'].astype(str)
        df_smd_indexed = df_smd_lookup.set_index('barcode')
        print(f"SMD lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned SMD lookup. Cannot perform SMD lookups.")


    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with final central file processing (Step 3)."
    if 'Barcode' not in updated_existing_central_df.columns or 'Status' not in updated_existing_central_df.columns:
        return False, "Error: 'Barcode' or 'Status' column not found in the updated central file. Cannot update status (Step 3)."

    consolidated_barcodes_set = set(consolidated_df['Barcode'].unique())
    central_barcodes_set = set(updated_existing_central_df['Barcode'].unique())

    barcodes_to_add = consolidated_barcodes_set - central_barcodes_set
    print(f"Found {len(barcodes_to_add)} new barcodes in consolidated file to add to central.")

    df_new_records_from_consolidated = consolidated_df[consolidated_df['Barcode'].isin(barcodes_to_add)].copy()

    all_new_central_rows_data = []

    for index, row_consolidated in df_new_records_from_consolidated.iterrows():
        barcode = row_consolidated['Barcode']
        channel = row_consolidated['Channel']

        vendor_name = row_consolidated.get('Vendor Name')
        vendor_number = row_consolidated.get('Vendor number')
        company_code = row_consolidated.get('Company code')
        received_date = row_consolidated.get('Received Date')
        processor = row_consolidated.get('Processor')
        category = row_consolidated.get('Category')
        region = row_consolidated.get('Region') # Added region from consolidated
        requester = row_consolidated.get('Requester') # Added requester from consolidated

        # --- PISA Lookup ---
        if channel == 'PISA' and not df_pisa_indexed.empty and barcode in df_pisa_indexed.index:
            pisa_row = df_pisa_indexed.loc[barcode]
            if 'vendor_name' in pisa_row.index and pd.notna(pisa_row['vendor_name']):
                vendor_name = pisa_row['vendor_name']
            if 'vendor_number' in pisa_row.index and pd.notna(pisa_row['vendor_number']):
                vendor_number = pisa_row['vendor_number']
            if 'company_code' in pisa_row.index and pd.notna(pisa_row['company_code']):
                company_code = pisa_row['company_code']
            if 'received_date' in pisa_row.index and pd.notna(pisa_row['received_date']):
                received_date = pisa_row['received_date']

        # --- ESM Lookup ---
        elif channel == 'ESM' and not df_esm_indexed.empty and barcode in df_esm_indexed.index:
            esm_row = df_esm_indexed.loc[barcode]
            if 'company_code' in esm_row.index and pd.notna(esm_row['company_code']):
                company_code = esm_row['company_code']
            if 'subcategory' in esm_row.index and pd.notna(esm_row['subcategory']):
                category = esm_row['subcategory']
            if 'vendor_name' in esm_row.index and pd.notna(esm_row['vendor_name']):
                vendor_name = esm_row['vendor_name']
            if 'vendor_number' in esm_row.index and pd.notna(esm_row['vendor_number']):
                vendor_number = esm_row['vendor_number']
            if 'received_date' in esm_row.index and pd.notna(esm_row['received_date']):
                received_date = esm_row['received_date']

        # --- PM7 Lookup ---
        elif channel == 'PM7' and not df_pm7_indexed.empty and barcode in df_pm7_indexed.index:
            pm7_row = df_pm7_indexed.loc[barcode]
            if 'vendor_name' in pm7_row.index and pd.notna(pm7_row['vendor_name']):
                vendor_name = pm7_row['vendor_name']
            if 'vendor_number' in pm7_row.index and pd.notna(pm7_row['vendor_number']):
                vendor_number = pm7_row['vendor_number']
            if 'company_code' in pm7_row.index and pd.notna(pm7_row['company_code']):
                company_code = pm7_row['company_code']
            if 'received_date' in pm7_row.index and pd.notna(pm7_row['received_date']):
                received_date = pm7_row['received_date']

        # --- SMD Lookup --- ADDED
        elif channel == 'SMD' and not df_smd_indexed.empty and barcode in df_smd_indexed.index:
            smd_row = df_smd_indexed.loc[barcode]
            if 'ekorg' in smd_row.index and pd.notna(smd_row['ekorg']):
                company_code = smd_row['ekorg']
            if 'material_field' in smd_row.index and pd.notna(smd_row['material_field']):
                region = smd_row['material_field']
            if 'pmd_sno' in smd_row.index and pd.notna(smd_row['pmd_sno']):
                vendor_number = smd_row['pmd_sno']
            if 'supplier_name' in smd_row.index and pd.notna(smd_row['supplier_name']):
                vendor_name = smd_row['supplier_name']
            if 'request_date' in smd_row.index and pd.notna(smd_row['request_date']):
                received_date = smd_row['request_date']
            if 'requested_by' in smd_row.index and pd.notna(smd_row['requested_by']):
                requester = smd_row['requested_by']

        new_central_row_data = row_consolidated.to_dict()
        new_central_row_data['Vendor Name'] = vendor_name if vendor_name is not None else ''
        new_central_row_data['Vendor number'] = vendor_number if vendor_number is not None else ''
        new_central_row_data['Company code'] = company_code if company_code is not None else ''
        new_central_row_data['Received Date'] = received_date
        new_central_row_data['Status'] = 'New'
        new_central_row_data['Allocation Date'] = datetime.now().strftime("%m/%d/%Y")
        new_central_row_data['Processor'] = processor if processor is not None else ''
        new_central_row_data['Category'] = category if category is not None else ''
        new_central_row_data['Region'] = region if region is not None else '' # Use lookup result
        new_central_row_data['Requester'] = requester if requester is not None else '' # Use lookup result


        all_new_central_rows_data.append(new_central_row_data)

    if all_new_central_rows_data:
        df_new_central_rows = pd.DataFrame(all_new_central_rows_data)
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_new_central_rows.columns:
                df_new_central_rows[col] = None
        df_new_central_rows = df_new_central_rows[CONSOLIDATED_OUTPUT_COLUMNS]
    else:
        df_new_central_rows = pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    for col in df_new_central_rows.columns:
        if df_new_central_rows[col].dtype == 'object':
            df_new_central_rows[col] = df_new_central_rows[col].fillna('')
        elif col in ['Barcode', 'Company code', 'Vendor number', 'Aging']: # Added Aging here
            df_new_central_rows[col] = df_new_central_rows[col].astype(str).replace('nan', '')

    barcodes_for_needs_review = central_barcodes_set - consolidated_barcodes_set
    print(f"Found {len(barcodes_for_needs_review)} barcodes in central not in consolidated.")

    df_final_central = updated_existing_central_df.copy()

    needs_review_barcode_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    is_not_completed_status_mask = ~df_final_central['Status'].astype(str).str.strip().str.lower().eq('completed')
    final_needs_review_condition = needs_review_barcode_mask & is_not_completed_status_mask

    df_final_central.loc[final_needs_review_condition, 'Status'] = 'Needs Review'
    print(f"Updated {final_needs_review_condition.sum()} records to 'Needs Review' where status was not 'Completed'.")

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = None
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

    df_final_central = pd.concat([df_final_central, df_new_central_rows], ignore_index=True)

    # --- NEW LOGIC START: Handle blank Company Code for PM7 channel ---
    print("\n--- Applying PM7 Company Code population logic ---")
    if 'Channel' in df_final_central.columns and 'Company code' in df_final_central.columns and 'Barcode' in df_final_central.columns:
        pm7_blank_cc_mask = (df_final_central['Channel'] == 'PM7') & \
                            (df_final_central['Company code'].astype(str).replace('nan', '').str.strip() == '')

        # Apply logic: take first 4 digits of Barcode
        df_final_central.loc[pm7_blank_cc_mask, 'Company code'] = \
            df_final_central.loc[pm7_blank_cc_mask, 'Barcode'].astype(str).str[:4]

        print(f"Populated Company Code for {pm7_blank_cc_mask.sum()} PM7 records based on Barcode.")
    else:
        print("Warning: 'Channel', 'Company code', or 'Barcode' columns missing. Skipping PM7 Company Code population logic.")
    # --- NEW LOGIC END ---

    # --- NEW REGION MAPPING LOGIC ---
    print("\n--- Applying Region Mapping ---")
    if region_mapping_df is None or region_mapping_df.empty:
        print("Warning: Region mapping file not provided or is empty. Region column will not be populated by external mapping.")
        # Ensure 'Region' is filled if not already done by a source file like SMD
        df_final_central['Region'] = df_final_central['Region'].fillna('')
    else:
        region_mapping_df = clean_column_names(region_mapping_df.copy())
        if 'r3_coco' not in region_mapping_df.columns or 'region' not in region_mapping_df.columns:
            print("Error: Region mapping file must contain 'r3_coco' and 'region' columns after cleaning. Skipping region mapping.")
            df_final_central['Region'] = df_final_central['Region'].fillna('')
        else:
            region_map = {}
            for idx, row in region_mapping_df.iterrows():
                coco_key = str(row['r3_coco']).strip().upper()
                if coco_key:
                    region_map[coco_key[:4]] = str(row['region']).strip()

            print(f"Loaded {len(region_map)} unique R/3 CoCo -> Region mappings.")

            if 'Company code' in df_final_central.columns:
                # Only apply region mapping if Region is still empty,
                # allowing SMD's Material Field to take precedence if present
                empty_region_mask = df_final_central['Region'].astype(str).str.strip() == ''
                df_final_central.loc[empty_region_mask, 'Company code_temp'] = \
                    df_final_central.loc[empty_region_mask, 'Company code'].astype(str).str.strip().str.upper().str[:4]
                df_final_central.loc[empty_region_mask, 'Region'] = \
                    df_final_central.loc[empty_region_mask, 'Company code_temp'].map(region_map).fillna(df_final_central.loc[empty_region_mask, 'Region'])
                df_final_central = df_final_central.drop(columns=['Company code_temp'])
                df_final_central['Region'] = df_final_central['Region'].fillna('')
                print("Region mapping applied successfully to empty 'Region' cells.")
            else:
                print("Warning: 'Company code' column not found in final central DataFrame. Cannot apply region mapping.")
                df_final_central['Region'] = df_final_central['Region'].fillna('')

    date_cols_in_central_file = [
        'Received Date', 'Re-Open Date', 'Allocation Date',
        'Completion Date', 'Clarification Date', 'Today'
    ]
    for col in df_final_central.columns:
        if col in date_cols_in_central_file:
            df_final_central[col] = format_date_to_mdyyyy(df_final_central[col])
        elif df_final_central[col].dtype == 'object':
            df_final_central[col] = df_final_central[col].fillna('')
        elif col in ['Barcode', 'Vendor number', 'Aging']: # Added Aging here
            df_final_central[col] = df_final_central[col].astype(str).replace('nan', '')

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = ''

    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

    try:
        df_final_central.to_excel(final_central_output_file_path, index=False)
        print(f"Final central file (after Step 3) saved to: {final_central_output_file_path}")
        print(f"Total rows in final central file (after Step 3): {len(df_final_central)}")
    except Exception as e:
        return False, f"Error saving final central file (after Step 3): {e}"
    print(f"--- Central File Status Processing (Step 3) Complete ---")
    return True, "Central file processing (Step 3) successful"


# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    temp_dir = tempfile.mkdtemp(dir='/tmp')

    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    session.pop('temp_dir', None)

    session['temp_dir'] = temp_dir

    # Region mapping file is in the project root, so we go up one level from BASE_DIR ('api/')
    REGION_MAPPING_FILE_PATH = os.path.join(BASE_DIR, '..', 'company_code_region_mapping.xlsx')

    try:
        uploaded_files = {}
        # Added 'smd_file' to file_keys
        file_keys = ['pisa_file', 'esm_file', 'pm7_file', 'smd_file', 'central_file']
        for key in file_keys:
            if key not in request.files:
                flash(f'Missing file: "{key}". All five files are required.', 'error')
                return redirect(url_for('index'))
            file = request.files[key]
            if file.filename == '':
                flash(f'No selected file for "{key}". All five files are required.', 'error')
                return redirect(url_for('index'))

            # Check for file extension (case-insensitive)
            if file and file.filename.lower().endswith('.xlsx'):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)
                uploaded_files[key] = file_path
                flash(f'File "{filename}" uploaded successfully.', 'info')
            else:
                flash(f'Invalid file type for "{key}". Please upload an .xlsx file.', 'error')
                return redirect(url_for('index'))

        pisa_file_path = uploaded_files['pisa_file']
        esm_file_path = uploaded_files['esm_file']
        pm7_file_path = uploaded_files['pm7_file']
        smd_file_path = uploaded_files['smd_file'] # Added SMD file path
        initial_central_file_input_path = uploaded_files['central_file']

        df_pisa_original = None
        df_esm_original = None
        df_pm7_original = None
        df_smd_original = None # Added SMD original DataFrame
        df_region_mapping = None

        try:
            df_pisa_original = pd.read_excel(pisa_file_path)
            df_esm_original = pd.read_excel(esm_file_path)
            df_pm7_original = pd.read_excel(pm7_file_path)
            df_smd_original = pd.read_excel(smd_file_path) # Read SMD file

            if os.path.exists(REGION_MAPPING_FILE_PATH):
                df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
                print(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
            else:
                flash(f"Error: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty.", 'warning')
                df_region_mapping = pd.DataFrame(columns=['R/3 CoCo', 'Region'])

        except Exception as e:
            flash(f"Error loading one or more input Excel files or the region mapping file: {e}. Please ensure all files are valid .xlsx formats and the mapping file exists.", 'error')
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))


        today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")

        # --- Step 1: Consolidate Data ---
        consolidated_output_filename = f'ConsolidatedData_{today_str}.xlsx'
        consolidated_output_file_path = os.path.join(temp_dir, consolidated_output_filename)
        # Pass df_smd_original to consolidate_data_process
        success, result = consolidate_data_process(
            df_pisa_original, df_esm_original, df_pm7_original, df_smd_original, consolidated_output_file_path
        )

        if not success:
            flash(f'Consolidation Error: {result}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        df_consolidated = result
        flash('Data consolidation from the sources completed successfully!', 'success')
        session['consolidated_output_path'] = consolidated_output_file_path

        # --- Step 2: Update existing central file records based on consolidation ---
        success, result_df = process_central_file_step2_update_existing(
            df_consolidated, initial_central_file_input_path
        )
        if not success:
            flash(f'Central File Processing (Step 2) Error: {result_df}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        df_central_updated_existing = result_df

        # --- Step 3: Final Merge (Add new barcodes, mark 'Needs Review', and apply Region Mapping) ---
        final_central_output_filename = f'CentralFile_FinalOutput_{today_str}.xlsx'
        final_central_output_file_path = os.path.join(temp_dir, final_central_output_filename)
        # Pass df_smd_original to process_central_file_step3_final_merge_and_needs_review
        success, message = process_central_file_step3_final_merge_and_needs_review(
            df_consolidated, df_central_updated_existing, final_central_output_file_path,
            df_pisa_original, df_esm_original, df_pm7_original, df_smd_original, df_region_mapping
        )
        if not success:
            flash(f'Central File Processing (Step 3) Error: {message}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        flash('Central file finalized successfully!', 'success')
        session['central_output_path'] = final_central_output_file_path

        return render_template('index.html',
                                central_download_link=url_for('download_file', filename=os.path.basename(final_central_output_file_path))
                              )

    except Exception as e:
        flash(f'An unhandled error occurred during processing: {e}', 'error')
        import traceback
        traceback.print_exc()
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        session.pop('temp_dir', None)
        return redirect(url_for('index'))
    finally:
        pass


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    file_path_in_temp = None
    temp_dir = session.get('temp_dir')

    print(f"DEBUG: Download requested for filename: {filename}")
    print(f"DEBUG: Session temp_dir: {temp_dir}")
    print(f"DEBUG: Consolidated output path in session: {session.get('consolidated_output_path')}")
    print(f"DEBUG: Central output path in session: {session.get('central_output_path')}")

    if not temp_dir:
        print("DEBUG: temp_dir not found in session.")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

    consolidated_session_path = session.get('consolidated_output_path')
    central_session_path = session.get('central_output_path')

    if consolidated_session_path and os.path.basename(consolidated_session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        print(f"DEBUG: Matched consolidated file. Reconstructed path: {file_path_in_temp}")
    elif central_session_path and os.path.basename(central_session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        print(f"DEBUG: Matched final central file. Reconstructed path: {file_path_in_temp}")
    else:
        print(f"DEBUG: Filename '{filename}' did not match any known session output files.")

    if file_path_in_temp and os.path.exists(file_path_in_temp):
        print(f"DEBUG: File '{file_path_in_temp}' exists. Attempting to send.")
        try:
            response = send_file(
                file_path_in_temp,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            return response
        except Exception as e:
            print(f"ERROR: Exception while sending file '{file_path_in_temp}': {e}")
            flash(f'Error providing download: {e}. Please try again.', 'error')
            return redirect(url_for('index'))
    else:
        print(f"DEBUG: File '{filename}' not found for download or session data missing/expired. Full path attempted: {file_path_in_temp}")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

@app.route('/cleanup_session', methods=['GET'])
def cleanup_session():
    temp_dir = session.get('temp_dir')
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"DEBUG: Cleaned up temporary directory: {temp_dir}")
            flash('Temporary files cleaned up.', 'info')
        except OSError as e:
            print(f"ERROR: Error removing temporary directory {temp_dir}: {e}")
            flash(f'Error cleaning up temporary files: {e}', 'error')
    session.pop('temp_dir', None)
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
