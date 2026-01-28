# your_project_root/api/pmd_lookup_app/__init__.py

from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
import io
import logging
import os

# Create a Blueprint for the PMD Lookup app
# template_folder and static_folder are relative to the *main* app's configured template/static paths.
# For this structure:
# Main app's templates are 'your_project_root/templates'.
# This blueprint's templates will be found in 'your_project_root/templates/pmd_lookup'.
# Relative path from api/pmd_lookup_app/ to templates/pmd_lookup is ../../templates/pmd_lookup
# Relative path from api/pmd_lookup_app/ to static/ is ../../static
pmd_lookup_bp = Blueprint('pmd_lookup', __name__,
                          template_folder='../../templates/pmd_lookup',
                          static_folder='../../static')

# Logging
pmd_lookup_logger = logging.getLogger('pmd_lookup_app')
pmd_lookup_logger.setLevel(logging.INFO)
if not pmd_lookup_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    pmd_lookup_logger.addHandler(handler)


ALLOWED_EXTENSIONS = {'xls', 'xlsx'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@pmd_lookup_bp.route('/', methods=['GET'])
def pmd_lookup_index(): # Renamed from 'index' to avoid conflict
    # render_template looks in the template_folder specified in the Blueprint constructor.
    # So 'index.html' here refers to 'templates/pmd_lookup/index.html'
    return render_template('index.html')


@pmd_lookup_bp.route('/process', methods=['POST'])
def process_files():
    try:
        # -------------------- FILE VALIDATION --------------------
        if 'central_file' not in request.files or 'pmd_lookup_file' not in request.files:
            flash('Both files are required.', 'error')
            return redirect(url_for('pmd_lookup.pmd_lookup_index'))

        central_file = request.files['central_file']
        pmd_file = request.files['pmd_lookup_file']

        if central_file.filename == '' or pmd_file.filename == '':
            flash('Please select both files.', 'error')
            return redirect(url_for('pmd_lookup.pmd_lookup_index'))

        if not (allowed_file(central_file.filename) and allowed_file(pmd_file.filename)):
            flash('Only Excel files (.xls, .xlsx) are allowed.', 'error')
            return redirect(url_for('pmd_lookup.pmd_lookup_index'))

        # -------------------- READ FILES --------------------
        central_df = pd.read_excel(io.BytesIO(central_file.read()))
        pmd_df = pd.read_excel(io.BytesIO(pmd_file.read()))

        # -------------------- REQUIRED COLUMNS --------------------
        central_required = ['Valid From', 'Supplier Name', 'Status', 'Assigned']
        pmd_required = ['Valid From', 'Supplier Name']

        for col in central_required:
            if col not in central_df.columns:
                raise KeyError(f"Central file missing column: '{col}'")

        for col in pmd_required:
            if col not in pmd_df.columns:
                raise KeyError(f"PMD file missing column: '{col}'")

        # -------------------- DATE NORMALIZATION --------------------
        central_df['Valid From_dt'] = pd.to_datetime(central_df['Valid From'], errors='coerce')
        pmd_df['Valid From_dt'] = pd.to_datetime(pmd_df['Valid From'], errors='coerce')

        central_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        pmd_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)

        # -------------------- CREATE MATCH KEY --------------------
        central_df['comp_key'] = (
            central_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' +
            central_df['Supplier Name'].astype(str).str.strip()
        )

        pmd_df['comp_key'] = (
            pmd_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' +
            pmd_df['Supplier Name'].astype(str).str.strip()
        )

        # -------------------- CENTRAL LOOKUP (NO JOIN) --------------------
        # Set index ensures quick lookup. It is possible for multiple rows to have the same comp_key
        # If this happens, .loc will return a Series, which needs to be handled.
        central_lookup = central_df.set_index('comp_key')[['Status', 'Assigned']]


        # -------------------- BUSINESS LOGIC --------------------
        def determine_status(row):
            # No match → New
            if row['comp_key'] not in central_lookup.index:
                return 'New', None

            lookup_result = central_lookup.loc[row['comp_key']]

            central_status = None
            central_assigned = None

            if isinstance(lookup_result, pd.DataFrame): # Multiple matches
                # Prioritize first non-null status/assigned if multiple matches exist
                central_status = lookup_result['Status'].dropna().iloc[0] if not lookup_result['Status'].dropna().empty else None
                central_assigned = lookup_result['Assigned'].dropna().iloc[0] if not lookup_result['Assigned'].dropna().empty else None
            else: # Single match (lookup_result is a Series)
                central_status = lookup_result['Status']
                central_assigned = lookup_result['Assigned']

            # Match + Approved → Ignore
            if isinstance(central_status, str) and central_status.lower() == 'approved':
                return None, None

            # Match + Not Approved → Hold
            return 'Hold', central_assigned

        pmd_df[['Status', 'Assigned']] = pmd_df.apply(
            lambda r: determine_status(r),
            axis=1,
            result_type='expand'
        )

        # Remove ignored rows
        final_df = pmd_df[pmd_df['Status'].notna()].copy()

        # -------------------- FORMAT & OUTPUT --------------------
        final_df['Valid From'] = final_df['Valid From_dt'].dt.strftime('%Y-%m-%d %I:%M %p')

        output_columns = [
            'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street',
            'City', 'Country', 'Zip Code', 'Requested By', 'Pur. approver',
            'Pur. release date', 'Status', 'Assigned'
        ]

        # Filter output_columns to only include those actually present in final_df
        final_df = final_df[[col for col in output_columns if col in final_df.columns]]

        # -------------------- CREATE EXCEL --------------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Result')

        output.seek(0)

        flash('File processed successfully!', 'success')
        return send_file(
            output,
            as_attachment=True,
            download_name='PMD_Lookup_Result.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except KeyError as ke:
        pmd_lookup_logger.error(f"Missing required column: {ke}", exc_info=True)
        flash(f"Error: A required column is missing in one of the files. Details: {ke}", 'error')
        return redirect(url_for('pmd_lookup.pmd_lookup_index'))
    except Exception as e:
        pmd_lookup_logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        flash(f"An unexpected error occurred: {e}", 'error')
        return redirect(url_for('pmd_lookup.pmd_lookup_index'))
