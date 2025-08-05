from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'gold_price_to_sheets',
    default_args=default_args,
    description='Extract gold price data from source and write to Google Sheets',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['etl', 'gold_price', 'google_sheets'],
)

def extract_milli_gold_price_data(**context):
    """
    Extract gold price data from source database for 'milli' source
    """
    try:
        # Source database connection
        source_hook = PostgresHook(
            postgres_conn_id='source_crypto_bot',
            schema='crypto_bot'
        )
        
        # SQL query to extract data
        query = """
        SELECT *,
            created_at + INTERVAL '3 hours 30 minutes' AS created_at_tehran
        FROM gold_price
        WHERE source = 'milli'
        ORDER BY id;
        """
        
        # Execute query and get data
        df = source_hook.get_pandas_df(query)
        
        # Convert timestamp columns to strings for JSON serialization
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif 'datetime' in str(df[col].dtype):
                df[col] = df[col].astype(str)
        
        # Convert DataFrame to records and handle any remaining timestamp issues
        records = []
        for _, row in df.iterrows():
            record = {}
            for col, value in row.items():
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, pd.Timestamp):
                    record[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    record[col] = value
            records.append(record)
        
        # Log the number of records extracted
        logging.info(f"Extracted {len(records)} records from source database for 'milli' source")
        
        # Store data in XCom for next task
        context['task_instance'].xcom_push(key='milli_gold_price_data', value=records)
        
        return f"Successfully extracted {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise

def write_to_google_sheets(**context):
    """
    Write extracted data to Google Sheets
    """
    try:
        # Get data from previous task
        data = context['task_instance'].xcom_pull(task_ids='extract_milli_gold_price_data', key='milli_gold_price_data')
        
        if not data:
            logging.warning("No data to write to Google Sheets")
            return "No data to write"
        
        # Convert back to DataFrame
        df = pd.DataFrame(data)
        
        # Google Sheets configuration
        SPREADSHEET_ID = '1lrAtfld0U_lO0IbeOVIE1_Ztv6QZ90vpcU3M-2AS3kg'
        SHEET_NAME = 'Sheet4'
        
        # Setup Google Sheets authentication
        try:
            # Load credentials from service account file
            credentials = Credentials.from_service_account_file(
                '/opt/airflow/config/service-account.json',
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        except FileNotFoundError:
            logging.error("Google Sheets credentials file not found. Please add credentials to /opt/airflow/config/service-account.json")
            raise Exception("Google Sheets credentials not configured")
        
        # Create Google Sheets client
        client = gspread.authorize(credentials)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        # Get the worksheet
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        
        # Clear existing data (optional - remove if you want to append)
        worksheet.clear()
        
        # Prepare data for Google Sheets
        # Convert DataFrame to list of lists (including headers)
        headers = df.columns.tolist()
        data_rows = df.values.tolist()
        
        # Combine headers and data
        sheet_data = [headers] + data_rows
        
        # Write data to Google Sheets
        worksheet.update('A1', sheet_data)
        
        logging.info(f"Successfully wrote {len(df)} records to Google Sheets")
        return f"Successfully wrote {len(df)} records to Google Sheets"
        
    except Exception as e:
        logging.error(f"Error writing to Google Sheets: {str(e)}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_milli_gold_price_data',
    python_callable=extract_milli_gold_price_data,
    dag=dag,
)

write_sheets_task = PythonOperator(
    task_id='write_to_google_sheets',
    python_callable=write_to_google_sheets,
    dag=dag,
)

# Define task dependencies
extract_task >> write_sheets_task 