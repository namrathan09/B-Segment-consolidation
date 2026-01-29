import os
import pandas as pd
from datetime import datetime
import warnings
import shutil
import tempfile
import re
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename
import logging
from io import BytesIO
import base64

warnings.filterwarnings('ignore')

# --- Vercel Specific Path Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(BASE_DIR, '..', 'templates')
static_dir = os.path.join(BASE_DIR, '..', 'static')

# Initialize Flask app
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

# IMPORTANT: Ensure FLASK_SECRET_KEY is set in your Vercel project environment variables
# A strong, random key is crucial for session security.
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_secret_key_for_local_dev_only')
# Optional: Set session cookie path to root and secure for Vercel production
# app.config['SESSION_COOKIE_PATH'] = '/'
# app.config['SESSION_COOKIE_SECURE'] = True # Use True in production with HTTPS
# app.config['SESSION_COOKIE_HTTPONLY'] = True
# app.config['SESSION_COOKIE_SAMESITE'] = 'Lax' # Or 'Strict' for more security


# --- Global Variables ---
CONSOLIDATED_OUTPUT_COLUMNS = [
    'Barcode', 'Processor', 'Channel', 'Category', 'Company code', 'Region',
    'Vendor number', 'Vendor Name', 'Status', 'Received Date', 'Re-Open Date',
    'Allocation Date', 'Clarification Date', 'Completion Date', 'Requester',
    'Remarks', 'Aging', 'Today'
]

PMD_OUTPUT_COLUMNS_SHEET1 = [ # Specific columns for Sheet1 (original format)
    'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street', 'City',
    'Country', 'Zip Code', 'Requested By', 'Pur. approver', 'Pur. release date'
]


# --- Configure Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.StreamHandler() # Only stream handler for Vercel logs
                    ])
logger = logging.getLogger(__name__)


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

def format_date_to_pmddump(date_series):
    """
    Formats a pandas Series of dates to YYYY-MM-DD HH:MM AM/PM string format.
    Handles potential mixed types and NaT values.
    """
    datetime_series = pd.to_datetime(date_series, errors='coerce')
    formatted_series = datetime_series.apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M %p') if pd.notna(x) else ''
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

def find_column_robust(df, target_column_keywords):
    """
    Finds a column in a DataFrame that matches the target keywords,
    ignoring case, spaces, and matching only the initial word.
    """
    if df is None or df.empty: # Added check for None df
        return None

    df_cols = [str(col).lower() for col in df.columns]
    target_keywords_processed = str(target_column_keywords).strip().lower().split()[0] # Only first word

    for original_col in df.columns:
        cleaned_col = str(original_col).strip().lower()
        cleaned_col_first_word = cleaned_col.split('_')[0] if '_' in cleaned_col else cleaned_col.split(' ')[0]

        if cleaned_col_first_word == target_keywords_processed:
            return original_col
    return None

def clean_col_name_str(col_name):
    if col_name is None:
        return None
    col = str(col_name).strip().lower()
    col = re.sub(r'\s+', '_', col)
    col = re.sub(r'[^a-z0-9_]', '', col)
    col = col.strip('_')
    return col

# --- Data Processing Functions (Existing Channels) ---

def consolidate_data_process(df_pisa, df_esm, df_pm7):
    """
    Reads PISA, ESM, and PM7 Excel files (now passed as DFs), filters PISA, consolidates data.
    Returns the consolidated DataFrame.
    """
    logger.info("Starting data consolidation process for PISA, ESM, PM7...")

    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())

    allowed_pisa_users = ["Goswami Sonali", "Patil Jayapal Gowd", "Ranganath Chilamakuri","Sridhar Divya","Sunitha S","Varunkumar N"]
    if 'assigned_user' in df_pisa.columns:
        original_pisa_count = len(df_pisa)
        df_pisa_filtered = df_pisa[df_pisa['assigned_user'].isin(allowed_pisa_users)].copy()
        logger.info(f"PISA file filtered. Original records: {original_pisa_count}, Records after filter: {len(df_pisa_filtered)}")
    else:
        logger.warning("'assigned_user' column not found in PISA file (after cleaning). No filter applied.")
        df_pisa_filtered = df_pisa.copy()

    all_consolidated_rows = []
    today_date = datetime.now()

    # --- PISA Processing ---
    if 'barcode' not in df_pisa_filtered.columns:
        logger.error("Error: 'barcode' column not found in PISA file (after cleaning). Skipping PISA processing.")
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
        logger.info(f"Collected {len(df_pisa_filtered)} rows from PISA.")

    # --- ESM Processing ---
    if 'barcode' not in df_esm.columns:
        logger.error("Error: 'barcode' column not found in ESM file (after cleaning). Skipping ESM processing.")
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
        logger.info(f"Collected {len(df_esm)} rows from ESM.")

    # --- PM7 Processing ---
    if 'barcode' not in df_pm7.columns:
        logger.error("Error: 'barcode' column not found in PM7 file (after cleaning). Skipping PM7 processing.")
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
        logger.info(f"Collected {len(df_pm7)} rows from PM7.")

    if not all_consolidated_rows:
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS) # Return empty DF if no data

    df_consolidated = pd.DataFrame(all_consolidated_rows)
    logger.info("--- PISA, ESM, PM7 Consolidation Complete ---")
    return df_consolidated


def process_central_file_step2_update_existing(consolidated_df_pisa_esm_pm7, central_file_input_path):
    """
    Step 2: Updates status of *existing* central file records based on PISA/ESM/PM7 consolidated data.
    """
    logger.info(f"\n--- Starting Central File Status Processing (Step 2: Update Existing Barcodes from PISA/ESM/PM7) ---")

    try:
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())

        logger.info("PISA/ESM/PM7 Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading PISA/ESM/PM7 Consolidated (DF) or Central (file) for processing (Step 2): {e}"

    if 'Barcode' not in consolidated_df_pisa_esm_pm7.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with central file processing (Step 2)."
    if 'barcode' not in df_central_cleaned.columns or 'status' not in df_central_cleaned.columns:
        return False, "Error: 'barcode' or 'status' column not found in the central file after cleaning. Cannot update status (Step 2)."

    consolidated_df_pisa_esm_pm7['Barcode'] = consolidated_df_pisa_esm_pm7['Barcode'].astype(str)
    df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str)

    df_central_cleaned['Barcode_compare'] = df_central_cleaned['barcode']

    consolidated_barcodes_set = set(consolidated_df_pisa_esm_pm7['Barcode'].unique())
    logger.info(f"Found {len(consolidated_barcodes_set)} unique barcodes in the PISA/ESM/PM7 consolidated file for Step 2.")

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

    logger.info(f"Updated 'status' column in central file for Step 2 for {len(df_central_cleaned)} records.")

    try:
        # Re-map cleaned names back to desired output names
        common_cols_map = {
            'barcode': 'Barcode', 'channel': 'Channel', 'company_code': 'Company code',
            'vendor_name': 'Vendor Name', 'vendor_number': 'Vendor number',
            'received_date': 'Received Date', 're_open_date': 'Re-Open Date',
            'allocation_date': 'Allocation Date', 'completion_date': 'Completion Date',
            'requester': 'Requester', 'clarification_date': 'Clarification Date',
            'aging': 'Aging', 'today': 'Today', 'status': 'Status', 'remarks': 'Remarks',
            'region': 'Region', 'processor': 'Processor', 'category': 'Category'
        }

        # Only rename if the cleaned column exists in df_central_cleaned
        cols_to_rename = {k: v for k, v in common_cols_map.items() if k in df_central_cleaned.columns}
        df_central_cleaned.rename(columns=cols_to_rename, inplace=True)

        # Ensure all CONSOLIDATED_OUTPUT_COLUMNS are present, adding empty ones if missing
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_central_cleaned.columns:
                df_central_cleaned[col] = '' # Initialize with empty string for consistency

    except Exception as e:
        return False, f"Error processing central file (Step 2): {e}"
    logger.info(f"--- Central File Status Processing (Step 2) Complete ---")
    return True, df_central_cleaned[CONSOLIDATED_OUTPUT_COLUMNS] # Ensure output has all required columns

def process_central_file_step3_final_merge_and_needs_review(consolidated_df_pisa_esm_pm7, updated_existing_central_df, df_pisa_original, df_esm_original, df_pm7_original, region_mapping_df):
    """
    Step 3: Handles barcodes present only in PISA/ESM/PM7 consolidated (adds them as new)
            and barcodes present only in central (marks them as 'Needs Review' if not 'Completed').
            Also performs region mapping.
    """
    logger.info(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review from PISA/ESM/PM7) ---")

    df_pisa_lookup = clean_column_names(df_pisa_original.copy())
    df_esm_lookup = clean_column_names(df_esm_original.copy())
    df_pm7_lookup = clean_column_names(df_pm7_original.copy())

    df_pisa_indexed = pd.DataFrame()
    if 'barcode' in df_pisa_lookup.columns:
        df_pisa_lookup['barcode'] = df_pisa_lookup['barcode'].astype(str)
        df_pisa_indexed = df_pisa_lookup.set_index('barcode')
        logger.info(f"PISA lookup indexed by 'barcode'.")
    else:
        logger.warning("Warning: 'barcode' column not found in cleaned PISA lookup. Cannot perform PISA lookups.")

    df_esm_indexed = pd.DataFrame()
    if 'barcode' in df_esm_lookup.columns:
        df_esm_lookup['barcode'] = df_esm_lookup['barcode'].astype(str)
        df_esm_indexed = df_esm_lookup.set_index('barcode')
        logger.info(f"ESM lookup indexed by 'barcode'.")
    else:
        logger.warning("Warning: 'barcode' column not found in cleaned ESM lookup. Cannot perform ESM lookups.")

    df_pm7_indexed = pd.DataFrame()
    if 'barcode' in df_pm7_lookup.columns:
        df_pm7_lookup['barcode'] = df_pm7_lookup['barcode'].astype(str)
        df_pm7_indexed = df_pm7_lookup.set_index('barcode')
        logger.info(f"PM7 lookup indexed by 'barcode'.")
    else:
        logger.warning("Warning: 'barcode' column not found in cleaned PM7 lookup. Cannot perform PM7 lookups.")

    if 'Barcode' not in consolidated_df_pisa_esm_pm7.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with final central file processing (Step 3)."
    if 'Barcode' not in updated_existing_central_df.columns or 'Status' not in updated_existing_central_df.columns:
        return False, "Error: 'Barcode' or 'Status' column not found in the updated central file. Cannot update status (Step 3)."

    consolidated_barcodes_set = set(consolidated_df_pisa_esm_pm7['Barcode'].unique())
    central_barcodes_set = set(updated_existing_central_df['Barcode'].unique())

    barcodes_to_add = consolidated_barcodes_set - central_barcodes_set
    logger.info(f"Found {len(barcodes_to_add)} new barcodes in PISA/ESM/PM7 consolidated file to add to central.")

    df_new_records_from_consolidated = consolidated_df_pisa_esm_pm7[consolidated_df_pisa_esm_pm7['Barcode'].isin(barcodes_to_add)].copy()

    all_new_central_rows_data = []
    today_date_formatted = datetime.now().strftime("%m/%d/%Y")

    for index, row_consolidated in df_new_records_from_consolidated.iterrows():
        barcode = row_consolidated['Barcode']
        channel = row_consolidated['Channel']

        # Get existing values from consolidated row as base
        new_central_row_data = row_consolidated.to_dict()

        # Update specific fields if they are blank and a lookup is possible
        if channel == 'PISA' and not df_pisa_indexed.empty and barcode in df_pisa_indexed.index:
            pisa_row = df_pisa_indexed.loc[barcode]
            if 'vendor_name' in pisa_row.index and pd.notna(pisa_row['vendor_name']) and not new_central_row_data.get('Vendor Name'):
                new_central_row_data['Vendor Name'] = pisa_row['vendor_name']
            if 'vendor_number' in pisa_row.index and pd.notna(pisa_row['vendor_number']) and not new_central_row_data.get('Vendor number'):
                new_central_row_data['Vendor number'] = pisa_row['vendor_number']
            if 'company_code' in pisa_row.index and pd.notna(pisa_row['company_code']) and not new_central_row_data.get('Company code'):
                new_central_row_data['Company code'] = pisa_row['company_code']
            if 'received_date' in pisa_row.index and pd.notna(pisa_row['received_date']) and not new_central_row_data.get('Received Date'):
                new_central_row_data['Received Date'] = pisa_row['received_date']

        elif channel == 'ESM' and not df_esm_indexed.empty and barcode in df_esm_indexed.index:
            esm_row = df_esm_indexed.loc[barcode]
            if 'company_code' in esm_row.index and pd.notna(esm_row['company_code']) and not new_central_row_data.get('Company code'):
                new_central_row_data['Company code'] = esm_row['company_code']
            if 'subcategory' in esm_row.index and pd.notna(esm_row['subcategory']) and not new_central_row_data.get('Category'):
                new_central_row_data['Category'] = esm_row['subcategory']
            if 'vendor_name' in esm_row.index and pd.notna(esm_row['vendor_name']) and not new_central_row_data.get('Vendor Name'):
                new_central_row_data['Vendor Name'] = esm_row['vendor_name']
            if 'vendor_number' in esm_row.index and pd.notna(esm_row['vendor_number']) and not new_central_row_data.get('Vendor number'):
                new_central_row_data['Vendor number'] = esm_row['vendor_number']
            if 'received_date' in esm_row.index and pd.notna(esm_row['received_date']) and not new_central_row_data.get('Received Date'):
                new_central_row_data['Received Date'] = esm_row['received_date'] # Corrected from pisa_row to esm_row

        elif channel == 'PM7' and not df_pm7_indexed.empty and barcode in df_pm7_indexed.index:
            pm7_row = df_pm7_indexed.loc[barcode]
            if 'vendor_name' in pm7_row.index and pd.notna(pm7_row['vendor_name']) and not new_central_row_data.get('Vendor Name'):
                new_central_row_data['Vendor Name'] = pm7_row['vendor_name']
            if 'vendor_number' in pm7_row.index and pd.notna(pm7_row['vendor_number']) and not new_central_row_data.get('Vendor number'):
                new_central_row_data['Vendor number'] = pm7_row['vendor_number']
            if 'company_code' in pm7_row.index and pd.notna(pm7_row['company_code']) and not new_central_row_data.get('Company code'):
                new_central_row_data['Company code'] = pm7_row['company_code']
            if 'received_date' in pm7_row.index and pd.notna(pm7_row['received_date']) and not new_central_row_data.get('Received Date'):
                new_central_row_data['Received Date'] = pm7_row['received_date']

        # Set or override specific values for new records
        new_central_row_data['Status'] = 'New'
        new_central_row_data['Allocation Date'] = today_date_formatted # Only for new records

        all_new_central_rows_data.append(new_central_row_data)

    if all_new_central_rows_data:
        df_new_central_rows = pd.DataFrame(all_new_central_rows_data)
        # Ensure all output columns are present in df_new_central_rows
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_new_central_rows.columns:
                df_new_central_rows[col] = None # Or appropriate default
        df_new_central_rows = df_new_central_rows[CONSOLIDATED_OUTPUT_COLUMNS]
    else:
        df_new_central_rows = pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    for col in df_new_central_rows.columns:
        if df_new_central_rows[col].dtype == 'object':
            df_new_central_rows[col] = df_new_central_rows[col].fillna('')
        elif col in ['Barcode', 'Company code', 'Vendor number']:
            df_new_central_rows[col] = df_new_central_rows[col].astype(str).replace('nan', '')

    barcodes_for_needs_review = central_barcodes_set - consolidated_barcodes_set
    logger.info(f"Found {len(barcodes_for_needs_review)} barcodes in central not in PISA/ESM/PM7 consolidated.")

    df_final_central = updated_existing_central_df.copy()

    needs_review_barcode_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    is_not_completed_status_mask = ~df_final_central['Status'].astype(str).str.strip().str.lower().eq('completed')
    final_needs_review_condition = needs_review_barcode_mask & is_not_completed_status_mask

    df_final_central.loc[final_needs_review_condition, 'Status'] = 'Needs Review'
    logger.info(f"Updated {final_needs_review_condition.sum()} records to 'Needs Review' where status was not 'Completed'.")

    # Ensure all CONSOLIDATED_OUTPUT_COLUMNS are present in df_final_central before concat
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = ''

    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS] # Reorder columns

    df_final_central = pd.concat([df_final_central, df_new_central_rows], ignore_index=True)

    # --- PM7 Company Code population logic ---
    logger.info("\n--- Applying PM7 Company Code population logic ---")
    if 'Channel' in df_final_central.columns and 'Company code' in df_final_central.columns and 'Barcode' in df_final_central.columns:
        pm7_blank_cc_mask = (df_final_central['Channel'] == 'PM7') & \
                            (df_final_central['Company code'].astype(str).replace('nan', '').str.strip() == '')

        df_final_central.loc[pm7_blank_cc_mask, 'Company code'] = \
            df_final_central.loc[pm7_blank_cc_mask, 'Barcode'].astype(str).str[:4]
        logger.info(f"Populated Company Code for {pm7_blank_cc_mask.sum()} PM7 records based on Barcode.")
    else:
        logger.warning("Warning: 'Channel', 'Company code', or 'Barcode' columns missing. Skipping PM7 Company Code population logic.")

    # --- REGION MAPPING LOGIC --- (Applied here for PISA/ESM/PM7 and then for Workon RGBA below)
    logger.info("\n--- Applying Region Mapping ---")
    if region_mapping_df is None or region_mapping_df.empty:
        logger.warning("Warning: Region mapping file not provided or is empty. Region column will not be populated for PISA/ESM/PM7 data.")
        df_final_central['Region'] = df_final_central['Region'].fillna('')
    else:
        region_mapping_df = clean_column_names(region_mapping_df.copy())
        if 'r3_coco' not in region_mapping_df.columns or 'region' not in region_mapping_df.columns:
            logger.error("Error: Region mapping file must contain 'r3_coco' and 'region' columns after cleaning. Skipping region mapping for PISA/ESM/PM7 data.")
            df_final_central['Region'] = df_final_central['Region'].fillna('')
        else:
            # Store the region_map for later use with RGBA and other data
            global_region_map = {}
            for idx, row in region_mapping_df.iterrows():
                coco_key = str(row['r3_coco']).strip().upper()
                if coco_key:
                    global_region_map[coco_key[:4]] = str(row['region']).strip()
            session['region_map'] = global_region_map # Store in session to pass to other functions

            logger.info(f"Loaded {len(global_region_map)} unique R/3 CoCo -> Region mappings.")

            if 'Company code' in df_final_central.columns:
                # Apply mapping to current df_final_central (PISA/ESM/PM7 data)
                df_final_central['Company code'] = df_final_central['Company code'].astype(str).str.strip().str.upper().str[:4]
                df_final_central['Region'] = df_final_central['Company code'].map(global_region_map).fillna(df_final_central['Region'])
                df_final_central['Region'] = df_final_central['Region'].fillna('')
                logger.info("Region mapping applied successfully to PISA/ESM/PM7 data.")
            else:
                logger.warning("Warning: 'Company code' column not found in PISA/ESM/PM7 consolidated DataFrame. Cannot apply region mapping.")
                df_final_central['Region'] = df_final_central['Region'].fillna('')

    # Format date columns after all data merges
    date_cols_to_format = [
        'Received Date', 'Re-Open Date', 'Allocation Date',
        'Completion Date', 'Clarification Date', 'Today'
    ]
    for col in df_final_central.columns:
        if col in date_cols_to_format:
            df_final_central[col] = format_date_to_mdyyyy(df_final_central[col])
        elif df_final_central[col].dtype == 'object':
            df_final_central[col] = df_final_central[col].fillna('')
        elif col in ['Barcode', 'Vendor number', 'Company code']: # Also ensure company code is treated as string
            df_final_central[col] = df_final_central[col].astype(str).replace('nan', '')

    logger.info(f"--- Central File Status Processing (Step 3) Complete ---")
    return True, df_final_central


def map_workon_columns(df_workon_raw):
    """
    Maps columns from the raw Workon P71 DataFrame to the CONSOLIDATED_OUTPUT_COLUMNS format.
    Handles robust column finding.
    """
    logger.info("\n--- Starting Workon P71 Data Mapping ---")
    if df_workon_raw.empty:
        logger.info("Workon P71 DataFrame is empty. Skipping mapping.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    df_workon = clean_column_names(df_workon_raw.copy()) # Clean column names for consistency
    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y")

    mapped_rows = []

    # Map Workon P71 columns to the standard output format
    # Use find_column_robust for flexible matching on the *original* column names
    workon_column_map = {
        'Barcode': find_column_robust(df_workon_raw, 'key'),
        'Category': find_column_robust(df_workon_raw, 'action'),
        'Company code': find_column_robust(df_workon_raw, 'company code'),
        'Region': find_column_robust(df_workon_raw, 'country'),
        'Vendor number': find_column_robust(df_workon_raw, 'vendor number'),
        'Vendor Name': find_column_robust(df_workon_raw, 'name'),
        'Status': find_column_robust(df_workon_raw, 'status'),
        'Received Date': find_column_robust(df_workon_raw, 'updated'), # Assuming 'updated' is the received date equivalent
        'Requester': find_column_robust(df_workon_raw, 'applicant'),
        'Remarks': find_column_robust(df_workon_raw, 'summary'),
    }

    # Validate essential columns
    # Barcode and Received Date are critical for new records
    # Status is also essential as it's a core output column and might be used in logic later
    if not all(workon_column_map[k] for k in ['Barcode', 'Status', 'Received Date']):
        missing_cols = [k for k, v in workon_column_map.items() if k in ['Barcode', 'Status', 'Received Date'] and v is None]
        logger.error(f"Error: Missing essential Workon P71 columns for mapping: {missing_cols}. Skipping Workon processing.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)


    for index, row in df_workon.iterrows(): # Iterate over the cleaned df
        new_row_data = {col: '' for col in CONSOLIDATED_OUTPUT_COLUMNS} # Initialize with blanks

        # Get values using the cleaned column names from the 'row' Series
        # Use .get() for robustness in case the column is somehow missing from a specific row (unlikely but safer)
        new_row_data['Barcode'] = str(row.get(clean_col_name_str(workon_column_map['Barcode']), '')) if workon_column_map['Barcode'] else ''
        new_row_data['Processor'] = 'Jayapal' # Hardcoded
        new_row_data['Channel'] = 'Workon' # Hardcoded (P71 and RGBA both use 'Workon' channel name)
        new_row_data['Category'] = str(row.get(clean_col_name_str(workon_column_map['Category']), '')) if workon_column_map['Category'] else ''
        new_row_data['Company code'] = str(row.get(clean_col_name_str(workon_column_map['Company code']), '')) if workon_column_map['Company code'] else ''
        new_row_data['Region'] = str(row.get(clean_col_name_str(workon_column_map['Region']), '')) if workon_column_map['Region'] else ''
        new_row_data['Vendor number'] = str(row.get(clean_col_name_str(workon_column_map['Vendor number']), '')) if workon_column_map['Vendor number'] else ''
        new_row_data['Vendor Name'] = str(row.get(clean_col_name_str(workon_column_map['Vendor Name']), '')) if workon_column_map['Vendor Name'] else ''
        new_row_data['Status'] = str(row.get(clean_col_name_str(workon_column_map['Status']), '')) if workon_column_map['Status'] else ''

        # Date columns - format immediately after retrieval
        received_date_val = row.get(clean_col_name_str(workon_column_map['Received Date'])) if workon_column_map['Received Date'] else None
        new_row_data['Received Date'] = format_date_to_mdyyyy(pd.Series([received_date_val])).iloc[0] if received_date_val is not None else ''


        new_row_data['Re-Open Date'] = '' # Blank
        new_row_data['Allocation Date'] = today_date_formatted # Today's Date
        new_row_data['Clarification Date'] = '' # Blank
        new_row_data['Completion Date'] = '' # Blank
        new_row_data['Requester'] = str(row.get(clean_col_name_str(workon_column_map['Requester']), '')) if workon_column_map['Requester'] else ''
        new_row_data['Remarks'] = str(row.get(clean_col_name_str(workon_column_map['Remarks']), '')) if workon_column_map['Remarks'] else ''
        new_row_data['Aging'] = '' # Blank
        new_row_data['Today'] = today_date_formatted # Today's Date

        mapped_rows.append(new_row_data)

    df_mapped_workon = pd.DataFrame(mapped_rows, columns=CONSOLIDATED_OUTPUT_COLUMNS)
    logger.info(f"Mapped {len(df_mapped_workon)} rows from Workon P71.")
    logger.info("--- Workon P71 Data Mapping Complete ---")
    return df_mapped_workon


def map_workon_rgba_columns(df_workon_rgba_raw, region_map):
    """
    Maps columns from the raw Workon RGBA DataFrame to the CONSOLIDATED_OUTPUT_COLUMNS format.
    Filters by 'Current Assignee' and handles robust column finding.
    Applies region mapping.
    """
    logger.info("\n--- Starting Workon RGBA Data Mapping ---")
    if df_workon_rgba_raw.empty:
        logger.info("Workon RGBA DataFrame is empty. Skipping mapping.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    df_workon_rgba = clean_column_names(df_workon_rgba_raw.copy()) # Clean column names for internal use
    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y")

    # --- Filtering Logic ---
    current_assignee_col_raw_name = find_column_robust(df_workon_rgba_raw, 'Current Assignee') # Use raw df for robust search
    if current_assignee_col_raw_name:
        # Get the cleaned name of the 'Current Assignee' column found
        cleaned_current_assignee_col = clean_col_name_str(current_assignee_col_raw_name)

        if cleaned_current_assignee_col in df_workon_rgba.columns:
            original_rgba_count = len(df_workon_rgba)
            df_workon_rgba_filtered = df_workon_rgba[
                df_workon_rgba[cleaned_current_assignee_col] == "VMD GS OSP-NA (GS/OMD-APAC)"
            ].copy()
            logger.info(f"Workon RGBA file filtered. Original records: {original_rgba_count}, Filtered records: {len(df_workon_rgba_filtered)}")
        else:
            logger.warning(f"Warning: Cleaned 'Current Assignee' column '{cleaned_current_assignee_col}' not found in Workon RGBA. No filter applied.")
            df_workon_rgba_filtered = df_workon_rgba.copy()
    else:
        logger.warning("Warning: 'Current Assignee' column not found in Workon RGBA file. No filter applied.")
        df_workon_rgba_filtered = df_workon_rgba.copy()

    if df_workon_rgba_filtered.empty:
        logger.info("Workon RGBA DataFrame is empty after filtering. Skipping mapping.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    mapped_rows = []

    # Map Workon RGBA columns to the standard output format
    # Use find_column_robust for flexible matching on the *original* column names
    workon_rgba_column_map = {
        'Barcode': find_column_robust(df_workon_rgba_raw, 'key'),
        'Company code': find_column_robust(df_workon_rgba_raw, 'company code'),
        'Received Date': find_column_robust(df_workon_rgba_raw, 'Updated'),
        'Remarks': find_column_robust(df_workon_rgba_raw, 'summary'),
        # These are currently blank in your request - will remain blank unless specified
        'Category': None,
        'Vendor number': None,
        'Vendor Name': None,
        'Status': None,
        'Requester': None,
    }

    # Validate essential columns
    if not all(workon_rgba_column_map[k] for k in ['Barcode', 'Received Date']): # Status is not mandatory for RGBA as per request
        missing_cols = [k for k, v in workon_rgba_column_map.items() if k in ['Barcode', 'Received Date'] and v is None]
        logger.error(f"Error: Missing essential Workon RGBA columns for mapping: {missing_cols}. Skipping Workon RGBA processing.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    for index, row in df_workon_rgba_filtered.iterrows(): # Iterate over the cleaned and filtered df
        new_row_data = {col: '' for col in CONSOLIDATED_OUTPUT_COLUMNS} # Initialize with blanks

        # Use .get() with the cleaned column names from the 'row' Series
        # We need to clean the name again for .get() because row.get expects the *actual* column name in the DataFrame
        new_row_data['Barcode'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Barcode']), '')) if workon_rgba_column_map['Barcode'] else ''
        new_row_data['Processor'] = 'Divya' # Hardcoded as per request
        new_row_data['Channel'] = 'Workon' # Hardcoded
        new_row_data['Category'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Category']), '')) if workon_rgba_column_map['Category'] else ''
        new_row_data['Company code'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Company code']), '')) if workon_rgba_column_map['Company code'] else ''
        
        new_row_data['Region'] = '' # Will be mapped after this loop based on Company Code

        new_row_data['Vendor number'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Vendor number']), '')) if workon_rgba_column_map['Vendor number'] else ''
        new_row_data['Vendor Name'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Vendor Name']), '')) if workon_rgba_column_map['Vendor Name'] else ''
        new_row_data['Status'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Status']), '')) if workon_rgba_column_map['Status'] else ''

        # Date columns - format immediately after retrieval
        received_date_val = row.get(clean_col_name_str(workon_rgba_column_map['Received Date'])) if workon_rgba_column_map['Received Date'] else None
        new_row_data['Received Date'] = format_date_to_mdyyyy(pd.Series([received_date_val])).iloc[0] if received_date_val is not None else ''

        new_row_data['Re-Open Date'] = '' # Blank
        new_row_data['Allocation Date'] = today_date_formatted # Today's Date
        new_row_data['Clarification Date'] = '' # Blank
        new_row_data['Completion Date'] = '' # Blank
        new_row_data['Requester'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Requester']), '')) if workon_rgba_column_map['Requester'] else ''
        new_row_data['Remarks'] = str(row.get(clean_col_name_str(workon_rgba_column_map['Remarks']), '')) if workon_rgba_column_map['Remarks'] else ''
        new_row_data['Aging'] = '' # Blank
        new_row_data['Today'] = today_date_formatted # Today's Date

        mapped_rows.append(new_row_data)

    df_mapped_workon_rgba = pd.DataFrame(mapped_rows, columns=CONSOLIDATED_OUTPUT_COLUMNS)

    # --- Apply Region Mapping to RGBA data ---
    if region_map and 'Company code' in df_mapped_workon_rgba.columns:
        df_mapped_workon_rgba['Company code'] = df_mapped_workon_rgba['Company code'].astype(str).str.strip().str.upper().str[:4]
        df_mapped_workon_rgba['Region'] = df_mapped_workon_rgba['Company code'].map(region_map).fillna(df_mapped_workon_rgba['Region'])
        df_mapped_workon_rgba['Region'] = df_mapped_workon_rgba['Region'].fillna('')
        logger.info(f"Region mapping applied to {len(df_mapped_workon_rgba)} Workon RGBA records.")
    else:
        logger.warning("Warning: Region mapping not applied to Workon RGBA data (mapping data not available or 'Company code' missing).")
        df_mapped_workon_rgba['Region'] = df_mapped_workon_rgba['Region'].fillna('') # Ensure it's not NaN

    logger.info(f"Mapped {len(df_mapped_workon_rgba)} rows from Workon RGBA.")
    logger.info("--- Workon RGBA Data Mapping Complete ---")
    return df_mapped_workon_rgba

def map_smd_columns(df_smd_raw):
    """
    Maps columns from the raw SMD DataFrame to the CONSOLIDATED_OUTPUT_COLUMNS format.
    Handles robust column finding and hardcoded values.
    """
    logger.info("\n--- Starting SMD Data Mapping ---")
    if df_smd_raw.empty:
        logger.info("SMD DataFrame is empty. Skipping mapping.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    df_smd = clean_column_names(df_smd_raw.copy())
    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y")

    mapped_rows = []

    smd_column_map = {
        'Company code': find_column_robust(df_smd_raw, 'Ekorg'),
        'Region': find_column_robust(df_smd_raw, 'Material Field'),
        'Vendor number': find_column_robust(df_smd_raw, 'PMD-SNO'),
        'Vendor Name': find_column_robust(df_smd_raw, 'supplier name'),
        'Received Date': find_column_robust(df_smd_raw, 'Request Date'),
        'Requester': find_column_robust(df_smd_raw, 'Requested by'),
        # Barcode is not mapped, so it will remain blank unless defined here
        # Processor, Category, Status, Re-Open Date, Clarification Date, Completion Date, Remarks, Aging
        # will be blank by default initialization
    }

    for index, row in df_smd.iterrows():
        new_row_data = {col: '' for col in CONSOLIDATED_OUTPUT_COLUMNS} # Initialize all with blank

        # Hardcoded values
        new_row_data['Channel'] = 'SMD'
        new_row_data['Allocation Date'] = today_date_formatted
        new_row_data['Today'] = today_date_formatted

        # Mapped values
        # Use clean_col_name_str on the robustly found raw column name
        new_row_data['Company code'] = str(row.get(clean_col_name_str(smd_column_map['Company code']), '')) if smd_column_map['Company code'] else ''
        new_row_data['Region'] = str(row.get(clean_col_name_str(smd_column_map['Region']), '')) if smd_column_map['Region'] else ''
        new_row_data['Vendor number'] = str(row.get(clean_col_name_str(smd_column_map['Vendor number']), '')) if smd_column_map['Vendor number'] else ''
        new_row_data['Vendor Name'] = str(row.get(clean_col_name_str(smd_column_map['Vendor Name']), '')) if smd_column_map['Vendor Name'] else ''
        new_row_data['Requester'] = str(row.get(clean_col_name_str(smd_column_map['Requester']), '')) if smd_column_map['Requester'] else ''

        # Date columns - format immediately after retrieval
        received_date_val = row.get(clean_col_name_str(smd_column_map['Received Date'])) if smd_column_map['Received Date'] else None
        new_row_data['Received Date'] = format_date_to_mdyyyy(pd.Series([received_date_val])).iloc[0] if received_date_val is not None else ''

        mapped_rows.append(new_row_data)

    df_mapped_smd = pd.DataFrame(mapped_rows, columns=CONSOLIDATED_OUTPUT_COLUMNS)
    logger.info(f"Mapped {len(df_mapped_smd)} rows from SMD.")
    logger.info("--- SMD Data Mapping Complete ---")
    return df_mapped_smd


# --- NEW PMD Lookup Processing Function (Now returns two DataFrames) ---
def pmd_lookup_process_function(df_pmd_dump_raw, df_pmd_central_raw):
    logger.info("\n--- Starting PMD Lookup Process ---")
    
    # Create copies to ensure original raw dataframes are not modified
    df_pmd_dump_working = df_pmd_dump_raw.copy() 
    df_pmd_central_working = df_pmd_central_raw.copy()

    # Clean column names for internal processing
    pmd_dump_df_cleaned_cols = clean_column_names(df_pmd_dump_working.copy()) 
    pmd_central_df_cleaned_cols = clean_column_names(df_pmd_central_working.copy())

    # --- Drop specified columns from PMD Dump (using cleaned names) ---
    cols_to_drop = ['sl_no', 'duns']
    for col in cols_to_drop:
        if col in pmd_dump_df_cleaned_cols.columns:
            pmd_dump_df_cleaned_cols.drop(columns=[col], inplace=True)
            logger.info(f"Dropped column '{col}' from PMD Dump.")
        else:
            logger.warning(f"Attempted to drop column '{col}' from PMD Dump, but it was not found.")

    # --- Filter out specific countries from PMD Dump (using cleaned names) ---
    excluded_countries = ['cn', 'id', 'tw', 'hk', 'jp', 'kr', 'my', 'ph', 'sg', 'th', 'vn']
    if 'country' in pmd_dump_df_cleaned_cols.columns:
        initial_dump_count = len(pmd_dump_df_cleaned_cols)
        pmd_dump_df_cleaned_cols = pmd_dump_df_cleaned_cols[~pmd_dump_df_cleaned_cols['country'].str.lower().isin(excluded_countries)].copy()
        logger.info(f"Filtered out {initial_dump_count - len(pmd_dump_df_cleaned_cols)} rows from PMD Dump based on excluded countries.")
    else:
        logger.warning("PMD Dump file is missing 'country' column. Cannot filter by country.")


    # --- Robust column finding for 'Valid From' and 'Supplier Name' for comparison keys ---
    # Need to find these columns in the *original* raw dataframes for find_column_robust
    valid_from_dump_col_raw = find_column_robust(df_pmd_dump_raw, 'Valid From')
    supplier_name_dump_col_raw = find_column_robust(df_pmd_dump_raw, 'Supplier Name')
    valid_from_central_col_raw = find_column_robust(df_pmd_central_raw, 'Valid From')
    supplier_name_central_col_raw = find_column_robust(df_pmd_central_raw, 'Supplier Name')

    required_pmd_cols = {
        'PMD Dump - Valid From': valid_from_dump_col_raw,
        'PMD Dump - Supplier Name': supplier_name_dump_col_raw,
        'PMD Central - Valid From': valid_from_central_col_raw,
        'PMD Central - Supplier Name': supplier_name_central_col_raw
    }

    missing_required_pmd_cols = [k for k, v in required_pmd_cols.items() if v is None]
    if missing_required_pmd_cols:
        logger.error(f"Missing one or more required columns for PMD Lookup comparison: {missing_required_pmd_cols}")
        return False, f"Missing one or more required columns for PMD Lookup comparison: {missing_required_pmd_cols}", None # Return 3 items for consistency

    # Get cleaned column names for internal use (from the *original* found names)
    valid_from_dump_col_cleaned = clean_col_name_str(valid_from_dump_col_raw)
    supplier_name_dump_col_cleaned = clean_col_name_str(supplier_name_dump_col_raw)
    valid_from_central_col_cleaned = clean_col_name_str(valid_from_central_col_raw)
    supplier_name_central_col_cleaned = clean_col_name_str(supplier_name_central_col_raw)


    # --- Prepare PMD Central for lookup ---
    pmd_central_df_cleaned_cols['valid_from_dt'] = pd.to_datetime(
        pmd_central_df_cleaned_cols.get(valid_from_central_col_cleaned), errors='coerce'
    )
    pmd_central_df_for_comp = pmd_central_df_cleaned_cols.dropna(
        subset=['valid_from_dt', supplier_name_central_col_cleaned]
    ).copy()
    pmd_central_df_for_comp['comp_key'] = (
        pmd_central_df_for_comp['valid_from_dt'].dt.strftime('%Y-%m-%d') +
        pmd_central_df_for_comp[supplier_name_central_col_cleaned].astype(str)
    )
    central_comp_keys = set(pmd_central_df_for_comp['comp_key'])
    logger.info(f"Prepared {len(central_comp_keys)} unique comparison keys from PMD Central file.")

    # --- Prepare PMD Dump for comparison ---
    pmd_dump_df_cleaned_cols['valid_from_dt'] = pd.to_datetime(
        pmd_dump_df_cleaned_cols.get(valid_from_dump_col_cleaned), errors='coerce'
    )
    pmd_dump_df_for_comp = pmd_dump_df_cleaned_cols.dropna(
        subset=['valid_from_dt', supplier_name_dump_col_cleaned]
    ).copy()
    pmd_dump_df_for_comp['comp_key'] = (
        pmd_dump_df_for_comp['valid_from_dt'].dt.strftime('%Y-%m-%d') +
        pmd_dump_df_for_comp[supplier_name_dump_col_cleaned].astype(str)
    )
    logger.info(f"Prepared {len(pmd_dump_df_for_comp)} rows for comparison from PMD Dump file.")

    # --- Find unique rows in PMD Dump not in PMD Central ---
    unique_dump_rows_for_sheet_generation = pmd_dump_df_for_comp[
        ~pmd_dump_df_for_comp['comp_key'].isin(central_comp_keys)
    ].copy()
    logger.info(f"Found {len(unique_dump_rows_for_sheet_generation)} unique rows in PMD Dump not present in PMD Central.")

    if unique_dump_rows_for_sheet_generation.empty:
        logger.info("No unique entries found in PMD Dump file compared to PMD Central file.")
        return False, "No unique entries found in PMD Dump file compared to PMD Central file.", None


    # --- Generate DataFrame for Sheet 1 (Original PMD format) ---
    df_sheet1 = pd.DataFrame()
    for col_name_output in PMD_OUTPUT_COLUMNS_SHEET1:
        # Robustly find the corresponding raw column name in the original PMD dump
        robust_found_raw_col = find_column_robust(df_pmd_dump_raw, col_name_output)
        cleaned_col_name_in_filtered = clean_col_name_str(robust_found_raw_col) if robust_found_raw_col else None
        
        if cleaned_col_name_in_filtered and cleaned_col_name_in_filtered in unique_dump_rows_for_sheet_generation.columns:
            df_sheet1[col_name_output] = unique_dump_rows_for_sheet_generation[cleaned_col_name_in_filtered]
        else:
            df_sheet1[col_name_output] = ''
            logger.warning(f"Sheet1 output column '{col_name_output}' not found or mapped from original PMD Dump. Added as blank.")
    
    # Format 'Valid From' for Sheet1
    if 'Valid From' in df_sheet1.columns:
        df_sheet1['Valid From'] = format_date_to_pmddump(df_sheet1['Valid From'])
    
    df_sheet1 = df_sheet1[PMD_OUTPUT_COLUMNS_SHEET1] # Ensure order


    # --- Generate DataFrame for Sheet 2 (Consolidated Output Format) ---
    df_sheet2_rows = []
    today_date_formatted = datetime.now().strftime("%m/%d/%Y")

    # Robustly find raw column names needed for Sheet2 mapping (use df_pmd_dump_raw)
    type_col_raw = find_column_robust(df_pmd_dump_raw, 'Type')
    bukr_col_raw = find_column_robust(df_pmd_dump_raw, 'Bukr.')
    country_col_raw = find_column_robust(df_pmd_dump_raw, 'Country')
    ebsno_col_raw = find_column_robust(df_pmd_dump_raw, 'EBSNO')
    supplier_name_col_raw = find_column_robust(df_pmd_dump_raw, 'Supplier Name')
    valid_from_col_raw_for_sheet2 = find_column_robust(df_pmd_dump_raw, 'Valid From') # Direct map from Valid From
    requested_by_col_raw = find_column_robust(df_pmd_dump_raw, 'Requested By')

    for index, row in unique_dump_rows_for_sheet_generation.iterrows(): # Iterate over the filtered dump data
        new_row_data = {col: '' for col in CONSOLIDATED_OUTPUT_COLUMNS} # Initialize all with blank

        # Hardcoded values
        new_row_data['Channel'] = 'PMD Lookup' # Specific channel name for this output
        new_row_data['Allocation Date'] = today_date_formatted
        new_row_data['Today'] = today_date_formatted

        # Mapped values from cleaned columns in 'row' (which is from unique_dump_rows_for_sheet_generation)
        new_row_data['Category'] = str(row.get(clean_col_name_str(type_col_raw), '')) if type_col_raw else ''
        new_row_data['Company code'] = str(row.get(clean_col_name_str(bukr_col_raw), '')) if bukr_col_raw else ''
        new_row_data['Region'] = str(row.get(clean_col_name_str(country_col_raw), '')) if country_col_raw else ''
        new_row_data['Vendor number'] = str(row.get(clean_col_name_str(ebsno_col_raw), '')) if ebsno_col_raw else ''
        new_row_data['Vendor Name'] = str(row.get(clean_col_name_str(supplier_name_col_raw), '')) if supplier_name_col_raw else ''
        new_row_data['Requester'] = str(row.get(clean_col_name_str(requested_by_col_raw), '')) if requested_by_col_raw else ''

        # Received Date logic - map from Valid From, and format with format_date_to_pmddump
        valid_from_val_for_sheet2 = row.get(clean_col_name_str(valid_from_col_raw_for_sheet2)) if valid_from_col_raw_for_sheet2 else None
        new_row_data['Received Date'] = format_date_to_pmddump(pd.Series([valid_from_val_for_sheet2])).iloc[0] if valid_from_val_for_sheet2 is not None else ''

        df_sheet2_rows.append(new_row_data)
    
    df_sheet2 = pd.DataFrame(df_sheet2_rows, columns=CONSOLIDATED_OUTPUT_COLUMNS)
    df_sheet2 = df_sheet2[CONSOLIDATED_OUTPUT_COLUMNS] # Ensure order


    logger.info("PMD Lookup Process completed successfully, generating two sheets.")
    return True, df_sheet1, df_sheet2 # Now return three items


# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    # Clear any previous download links when returning to index
    # Download links will only appear if the session has output_data and output_filename
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    consolidated_output_buffer = BytesIO() # Create a BytesIO object for the final output
    
    # Clear relevant session data for a fresh start for this process
    session.pop('central_output_data', None) # Store data directly, not path
    session.pop('central_output_filename', None)
    session.pop('region_map', None)
    
    temp_dir_for_uploads = tempfile.mkdtemp(dir='/tmp') # Temp dir for current request's uploads

    REGION_MAPPING_FILE_PATH = os.path.join(BASE_DIR, '..', 'company_code_region_mapping.xlsx')

    try:
        uploaded_files = {}
        
        # Mandatory files
        mandatory_file_keys = ['pisa_file', 'esm_file', 'pm7_file', 'central_file']
        # Optional files
        optional_file_keys = ['workon_file', 'workon_rgba_file', 'smd_data_file']

        # Process mandatory files first
        for key in mandatory_file_keys:
            if key not in request.files or request.files[key].filename == '':
                flash(f'Missing mandatory file: "{key}". All PISA, ESM, PM7, and Central files are required.', 'error')
                if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
                return redirect(url_for('index'))
            
            file = request.files[key]
            if file and file.filename.lower().endswith('.xlsx'):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir_for_uploads, filename)
                file.save(file_path)
                uploaded_files[key] = file_path
                logger.info(f'File "{filename}" uploaded successfully.')
            else:
                flash(f'Invalid file type for "{key}". Please upload an .xlsx file.', 'error')
                if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
                return redirect(url_for('index'))

        # Process optional files
        for key in optional_file_keys:
            file = request.files.get(key)
            if file and file.filename != '':
                if file.filename.lower().endswith('.xlsx'):
                    filename = secure_filename(file.filename)
                    file_path = os.path.join(temp_dir_for_uploads, filename)
                    file.save(file_path)
                    uploaded_files[key] = file_path
                    logger.info(f'Optional file "{filename}" uploaded successfully.')
                else:
                    flash(f'Invalid file type for optional file "{key}". It must be an .xlsx file, or left blank.', 'warning')
            else:
                logger.info(f"INFO: Optional file '{key}' not provided or empty. Skipping.")

        pisa_file_path = uploaded_files['pisa_file']
        esm_file_path = uploaded_files['esm_file']
        pm7_file_path = uploaded_files['pm7_file']
        initial_central_file_input_path = uploaded_files['central_file']
        
        workon_file_path = uploaded_files.get('workon_file')
        workon_rgba_file_path = uploaded_files.get('workon_rgba_file')
        smd_data_file_path = uploaded_files.get('smd_data_file')

        df_pisa_original = None
        df_esm_original = None
        df_pm7_original = None
        df_workon_original = None
        df_workon_rgba_original = None
        df_smd_original = None
        df_region_mapping = None

        try:
            df_pisa_original = pd.read_excel(pisa_file_path)
            df_esm_original = pd.read_excel(esm_file_path)
            df_pm7_original = pd.read_excel(pm7_file_path)
            
            if workon_file_path:
                df_workon_original = pd.read_excel(workon_file_path)
            if workon_rgba_file_path:
                df_workon_rgba_original = pd.read_excel(workon_rgba_file_path)
            if smd_data_file_path:
                df_smd_original = pd.read_excel(smd_data_file_path)

            if os.path.exists(REGION_MAPPING_FILE_PATH):
                df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
                logger.info(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
            else:
                flash(f"Error: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty.", 'warning')
                df_region_mapping = pd.DataFrame(columns=['R/3 CoCo', 'Region'])

        except Exception as e:
            flash(f"Error loading one or more input Excel files or the region mapping file: {e}. Please ensure all files are valid .xlsx formats and the mapping file exists.", 'error')
            logger.error(f"Error loading input files: {e}")
            if os.path.exists(temp_dir_for_uploads):
                shutil.rmtree(temp_dir_for_uploads)
            return redirect(url_for('index'))


        today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")

        # --- Phase 1: Consolidate PISA, ESM, PM7 data ---
        df_consolidated_pisa_esm_pm7 = consolidate_data_process(
            df_pisa_original, df_esm_original, df_pm7_original
        )

        if df_consolidated_pisa_esm_pm7.empty and (not df_pisa_original.empty or not df_esm_original.empty or not df_pm7_original.empty):
             flash('Consolidation from PISA/ESM/PM7 resulted in an empty dataset. Check input files and assigned users.', 'warning')
        elif not df_consolidated_pisa_esm_pm7.empty:
             flash('Data consolidation from PISA, ESM, PM7 completed successfully!', 'success')
        
        # --- Step 2: Update existing central file records based on PISA/ESM/PM7 consolidation ---
        success, df_central_updated_existing = process_central_file_step2_update_existing(
            df_consolidated_pisa_esm_pm7, initial_central_file_input_path
        )
        if not success:
            flash(f'Central File Processing (Step 2) Error: {df_central_updated_existing}', 'error')
            if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
            return redirect(url_for('index'))

        # --- Step 3: Final Merge (Add new PISA/ESM/PM7 barcodes, mark 'Needs Review', apply Region Mapping) ---
        success, df_final_central_pisa_esm_pm7 = process_central_file_step3_final_merge_and_needs_review(
            df_consolidated_pisa_esm_pm7, df_central_updated_existing, df_pisa_original, df_esm_original, df_pm7_original, df_region_mapping
        )
        if not success:
            flash(f'Central File Processing (Step 3) Error: {df_final_central_pisa_esm_pm7}', 'error')
            if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
            return redirect(url_for('index'))
        flash('Central file updated and merged with PISA, ESM, PM7 data successfully!', 'success')

        current_region_map = session.get('region_map', {})

        df_current_consolidated = df_final_central_pisa_esm_pm7.copy()

        # --- Workon P71 Integration (Append) ---
        if df_workon_original is not None and not df_workon_original.empty:
            df_mapped_workon_p71 = map_workon_columns(df_workon_original)
            if df_mapped_workon_p71.empty:
                flash('Workon P71 file was empty or had mapping issues. No Workon P71 data added.', 'warning')
            else:
                df_current_consolidated = pd.concat([df_current_consolidated, df_mapped_workon_p71[CONSOLIDATED_OUTPUT_COLUMNS]], ignore_index=True)
                flash('Workon P71 data successfully mapped and appended.', 'success')
        else:
            logger.info("INFO: Workon P71 file not provided or empty. Skipping processing.")


        # --- Workon RGBA Integration (Append) ---
        if df_workon_rgba_original is not None and not df_workon_rgba_original.empty:
            df_mapped_workon_rgba = map_workon_rgba_columns(df_workon_rgba_original, current_region_map)
            if df_mapped_workon_rgba.empty:
                flash('Workon RGBA file was empty, had filtering issues, or mapping issues. No Workon RGBA data added.', 'warning')
            else:
                df_current_consolidated = pd.concat([df_current_consolidated, df_mapped_workon_rgba[CONSOLIDATED_OUTPUT_COLUMNS]], ignore_index=True)
                flash('Workon RGBA data successfully filtered, mapped, and appended!', 'success')
        else:
            logger.info("INFO: Workon RGBA file not provided or empty. Skipping processing.")

        # --- SMD Data Integration (Append) ---
        if df_smd_original is not None and not df_smd_original.empty:
            df_mapped_smd = map_smd_columns(df_smd_original)
            if df_mapped_smd.empty:
                flash('SMD Data file was empty or had mapping issues. No SMD data added.', 'warning')
            else:
                df_current_consolidated = pd.concat([df_current_consolidated, df_mapped_smd[CONSOLIDATED_OUTPUT_COLUMNS]], ignore_index=True)
                flash('SMD Data successfully mapped and appended.', 'success')
        else:
            logger.info("INFO: SMD Data file not provided or empty. Skipping processing.")


        df_ultimate_final_central = df_current_consolidated


        # --- FINAL AGING CALCULATION ---
        logger.info("\n--- Calculating Aging for blank entries ---")
        # Convert date columns to datetime objects for calculation, coercing errors to NaT
        # Need 'Today' and 'Allocation Date' for the calculation
        df_ultimate_final_central['Today_dt'] = pd.to_datetime(df_ultimate_final_central['Today'], format='%m/%d/%Y', errors='coerce')
        df_ultimate_final_central['Allocation Date_dt'] = pd.to_datetime(df_ultimate_final_central['Allocation Date'], format='%m/%d/%Y', errors='coerce')

        # Identify rows where 'Aging' is blank/empty and both dates are valid for calculation
        aging_mask = (df_ultimate_final_central['Aging'].fillna('').astype(str).str.strip() == '') & \
                     (df_ultimate_final_central['Today_dt'].notna()) & \
                     (df_ultimate_final_central['Allocation Date_dt'].notna())

        # Calculate aging as (Today - Allocation Date) and convert to integer days
        df_ultimate_final_central.loc[aging_mask, 'Aging'] = \
            (df_ultimate_final_central.loc[aging_mask, 'Today_dt'] - \
             df_ultimate_final_central.loc[aging_mask, 'Allocation Date_dt']).dt.days.astype(str)

        # Clean up temporary date columns
        df_ultimate_final_central = df_ultimate_final_central.drop(columns=['Today_dt', 'Allocation Date_dt'])
        logger.info(f"Calculated Aging for {aging_mask.sum()} entries.")
        # --- END FINAL AGING CALCULATION ---


        # Final output saving to BytesIO
        final_central_output_filename = f'CentralFile_FinalOutput_{today_str}.xlsx'
        consolidated_output_buffer.seek(0) # Ensure buffer is at the start
        try:
            df_ultimate_final_central.to_excel(consolidated_output_buffer, index=False)
            consolidated_output_buffer.seek(0) # Rewind the buffer to the beginning after writing
        except Exception as e:
            flash(f"Error saving final central file to memory: {e}", 'error')
            logger.error(f"Error saving final central file to memory: {e}")
            if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
            return redirect(url_for('index'))

        # Encode the buffer content to base64 and store in session
        session['central_output_data'] = base64.b64encode(consolidated_output_buffer.getvalue()).decode('utf-8')
        session['central_output_filename'] = final_central_output_filename


        # Implement PRG pattern: Redirect to a GET endpoint
        return redirect(url_for('show_consolidated_result'))

    except Exception as e:
        flash(f'An unhandled error occurred during consolidated processing: {e}', 'error')
        logger.exception("Unhandled error during consolidated file processing:")
        if os.path.exists(temp_dir_for_uploads):
            shutil.rmtree(temp_dir_for_uploads)
        return redirect(url_for('index'))
    finally:
        # Clean up intermediate upload temp directory
        if os.path.exists(temp_dir_for_uploads):
            shutil.rmtree(temp_dir_for_uploads)
            logger.info(f"Cleaned up temporary upload directory: {temp_dir_for_uploads}")


# NEW GET endpoint to display download link after consolidated processing
@app.route('/show_consolidated_result', methods=['GET'])
def show_consolidated_result():
    # Check if download info is in session (set by /process POST)
    if 'central_output_data' in session and 'central_output_filename' in session:
        return render_template('index.html', central_download_link=url_for('download_consolidated_file'))
    else:
        # If no data found, something went wrong or session expired
        flash('No consolidated file results found. Please process files again.', 'error')
        return redirect(url_for('index'))


# --- NEW ROUTE FOR PMD LOOKUP ---
@app.route('/process_pmd_lookup', methods=['POST'])
def process_pmd_lookup():
    pmd_output_buffer = BytesIO() # Use BytesIO for in-memory handling for Vercel

    # Clear session data specific to PMD lookup for a fresh start
    session.pop('pmd_output_data', None) # Store data directly, not path
    session.pop('pmd_output_filename', None) # Store filename

    temp_dir_for_uploads = tempfile.mkdtemp(dir='/tmp') # Temp dir for current request's uploads

    try:
        pmd_dump_file = request.files.get('pmd_dump_file')
        pmd_central_file = request.files.get('pmd_central_file')

        if not pmd_dump_file or pmd_dump_file.filename == '':
            flash('Missing "PMD Dump file".', 'error')
            logger.error('Missing PMD Dump file.')
            return redirect(url_for('index'))
        if not pmd_central_file or pmd_central_file.filename == '':
            flash('Missing "PMD Central file".', 'error')
            logger.error('Missing PMD Central file.')
            return redirect(url_for('index'))

        if not pmd_dump_file.filename.lower().endswith('.xlsx') or \
           not pmd_central_file.filename.lower().endswith('.xlsx'):
            flash('Both PMD files must be .xlsx format.', 'error')
            logger.error('Invalid file format for PMD files. Must be .xlsx.')
            return redirect(url_for('index'))

        dump_path = os.path.join(temp_dir_for_uploads, secure_filename(pmd_dump_file.filename))
        central_path = os.path.join(temp_dir_for_uploads, secure_filename(pmd_central_file.filename))

        pmd_dump_file.save(dump_path)
        pmd_central_file.save(central_path)
        logger.info(f"PMD Dump file saved to {dump_path}")
        logger.info(f"PMD Central file saved to {central_path}")

        df_pmd_dump_raw = pd.read_excel(dump_path)
        df_pmd_central_raw = pd.read_excel(central_path)

        # Call the updated function that returns three items: success status, df_sheet1, df_sheet2
        success, df_sheet1_or_error_msg, df_sheet2 = pmd_lookup_process_function(df_pmd_dump_raw, df_pmd_central_raw)

        if not success:
            flash(f'PMD Lookup failed: {df_sheet1_or_error_msg}', 'error')
            logger.error(f"PMD Lookup process failed: {df_sheet1_or_error_msg}")
            return redirect(url_for('index'))

        # If successful, df_sheet1_or_error_msg is actually df_sheet1
        df_sheet1 = df_sheet1_or_error_msg

        today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")
        pmd_output_filename = f'PMD_Lookup_ResultFile_{today_str}.xlsx'
        
        try:
            # Use ExcelWriter to write to multiple sheets in memory
            with pd.ExcelWriter(pmd_output_buffer, engine='xlsxwriter') as writer:
                df_sheet1.to_excel(writer, sheet_name='Sheet1_PMD_Original', index=False)
                df_sheet2.to_excel(writer, sheet_name='Sheet2_Consolidated_Format', index=False)
            pmd_output_buffer.seek(0) # Rewind the buffer to the beginning after writing
            logger.info("PMD Lookup result file saved to memory with two sheets.")
            
        except Exception as e:
            flash(f"Error saving PMD Lookup result file to memory: {e}", 'error')
            logger.error(f"Error saving PMD Lookup result file: {e}")
            if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
            return redirect(url_for('index'))

        # Encode the buffer content to base64 and store in session
        session['pmd_output_data'] = base64.b64encode(pmd_output_buffer.getvalue()).decode('utf-8')
        session['pmd_output_filename'] = pmd_output_filename
        
        # Implement PRG pattern: Redirect to a GET endpoint
        return redirect(url_for('show_pmd_result'))

    except Exception as e:
        flash(f'An unhandled error occurred during PMD Lookup: {e}', 'error')
        logger.exception("Unhandled error during PMD Lookup processing:")
        if os.path.exists(temp_dir_for_uploads): shutil.rmtree(temp_dir_for_uploads)
        return redirect(url_for('index'))
    finally:
        # Clean up intermediate upload temp directory
        if os.path.exists(temp_dir_for_uploads):
            shutil.rmtree(temp_dir_for_uploads)
            logger.info(f"Cleaned up temporary upload directory: {temp_dir_for_uploads}")

# NEW GET endpoint to display download link after PMD processing
@app.route('/show_pmd_result', methods=['GET'])
def show_pmd_result():
    # Check if download info is in session (set by /process_pmd_lookup POST)
    if 'pmd_output_data' in session and 'pmd_output_filename' in session:
        return render_template('index.html', pmd_download_link=url_for('download_pmd_file'))
    else:
        # If no data found, something went wrong or session expired
        flash('No PMD lookup results found. Please process files again.', 'error')
        return redirect(url_for('index'))


@app.route('/download_consolidated_file', methods=['GET'])
def download_consolidated_file():
    # Retrieve base64 encoded data and filename from session
    encoded_output_data = session.get('central_output_data')
    output_filename = session.get('central_output_filename')

    if encoded_output_data is None or output_filename is None:
        logger.warning("Consolidated output data not found in session for download.")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))
    
    # Decode the base64 data back to bytes
    output_data = base64.b64decode(encoded_output_data)

    logger.info(f"Attempting to send consolidated file: {output_filename}")
    try:
        # Clear the session data AFTER successful retrieval and BEFORE sending
        session.pop('central_output_data', None)
        session.pop('central_output_filename', None)
        
        return send_file(
            BytesIO(output_data),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=output_filename
        )
    except Exception as e:
        logger.error(f"Exception while sending consolidated file '{output_filename}': {e}")
        flash(f'Error providing download: {e}. Please try again.', 'error')
        return redirect(url_for('index'))

@app.route('/download_pmd_file', methods=['GET'])
def download_pmd_file():
    # Retrieve base64 encoded data and filename from session
    encoded_output_data = session.get('pmd_output_data')
    output_filename = session.get('pmd_output_filename')

    if encoded_output_data is None or output_filename is None:
        logger.warning("PMD output data not found in session for download.")
        flash('PMD result file not found for download or session expired. Please re-run the PMD Lookup process.', 'error')
        return redirect(url_for('index'))
    
    # Decode the base64 data back to bytes
    output_data = base64.b64decode(encoded_output_data)

    logger.info(f"Attempting to send PMD lookup file: {output_filename}")
    try:
        # Clear the session data AFTER successful retrieval and BEFORE sending
        session.pop('pmd_output_data', None)
        session.pop('pmd_output_filename', None)

        return send_file(
            BytesIO(output_data),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=output_filename
        )
    except Exception as e:
        logger.error(f"Exception while sending PMD lookup file '{output_filename}': {e}")
        flash(f'Error providing PMD download: {e}. Please try again.', 'error')
        return redirect(url_for('index'))

@app.route('/cleanup_session', methods=['GET'])
def cleanup_session():
    # Pop all relevant session keys
    session.pop('central_output_data', None)
    session.pop('central_output_filename', None)
    session.pop('pmd_output_data', None)
    session.pop('pmd_output_filename', None)
    session.pop('region_map', None) # This can be cleaned up as well

    # Flash message to confirm cleanup
    flash('Session data cleaned up.', 'info')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
