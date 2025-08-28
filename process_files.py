import pandas as pd
from datetime import datetime
import io

def process_uploaded_files(uploaded_files):
    """
    Processes files uploaded via the web interface.

    Args:
        uploaded_files: A list of file-like objects from the web upload.

    Returns:
        A dictionary containing processed file streams and a log of operations.
    """
    log = []
    output_files = {}
    processed_files_count = 0

    log.append("--- Starting file processing... ---")

    for file_storage in uploaded_files:
        filename = file_storage.filename
        if filename.startswith("ALL_LS_COINS_") and filename.lower().endswith(('.xlsx', '.csv')):
            log.append(f"Processing file: {filename}...")
            
            try:
                # --- 1. Parse filename to get currency and date ---
                base_name = ".".join(filename.split('.')[:-1])
                parts = base_name.split(' ')
                if len(parts) < 2:
                    log.append(f"  -> SKIPPING: Filename '{filename}' has an unexpected format.")
                    continue
                    
                currency_code = parts[0].split('_')[-1]
                date_str_mmddyy = parts[1]
                date_obj = datetime.strptime(date_str_mmddyy, '%m.%d.%y')
                date_str_yyyymmdd = date_obj.strftime('%Y%m%d')

                # --- 2. Read file content from in-memory stream ---
                content = file_storage.read()
                if filename.lower().endswith('.xlsx'):
                    df = pd.read_excel(io.BytesIO(content), dtype=str)
                else:
                    df = pd.read_csv(io.BytesIO(content), dtype=str)

                df.columns = [str(col).strip() for col in df.columns]

                # --- 3. Verify required columns ---
                required_cols = ['USERID', 'COINS']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    log.append(f"  -> ERROR & SKIPPING: File '{filename}' is missing required column(s): {', '.join(missing_cols)}.")
                    continue
                
                log.append("  -> Found required columns: 'USERID', 'COINS'")

                # --- 4. Generate CSV file in memory ---
                df['COINS'] = pd.to_numeric(df['COINS'], errors='coerce').fillna(0).astype(int)
                df_csv = df[['USERID', 'COINS']]
                csv_filename = f"{currency_code}_DEP_{date_str_yyyymmdd}.csv"
                
                csv_buffer = io.StringIO()
                df_csv.to_csv(csv_buffer, index=False, header=False)
                output_files[csv_filename] = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
                log.append(f"  -> CSV created in memory: {csv_filename}")

                # --- 5. Generate XLSX file in memory ---
                df_xlsx = df[['USERID']]
                xlsx_filename = f"{currency_code} Lucky Spin Inbox Blast {date_str_yyyymmdd}.xlsx"

                xlsx_buffer = io.BytesIO()
                with pd.ExcelWriter(xlsx_buffer, engine='openpyxl') as writer:
                    df_xlsx.to_excel(writer, index=False, header=False, startrow=1)
                output_files[xlsx_filename] = xlsx_buffer
                log.append(f"  -> XLSX created in memory: {xlsx_filename}")

                processed_files_count += 1

            except Exception as e:
                log.append(f"  -> FATAL ERROR: An unexpected error occurred while processing {filename}: {e}")
    
    log.append("\n--- Processing Complete ---")
    if processed_files_count == 0:
        log.append("No files were processed. Please check filenames and column headers.")
    else:
        log.append(f"Successfully processed {processed_files_count} file(s).")
        log.append("Generated files are ready for download.")
        
    return output_files, "\n".join(log)