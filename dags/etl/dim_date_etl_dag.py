from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
import pandas as pd
import os

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
    'dim_date_etl',
    default_args=default_args,
    description='ETL for dim_date table - update calculated columns daily',
    schedule_interval='35 3 * * *',  # Daily at 3:35 AM
    catchup=False,
    tags=['dim_date', 'etl', 'daily']
)

def update_dim_date_calculated_columns(**context):
    """Update the calculated date columns in dim_date table"""
    
    pg_hook = PostgresHook(postgres_conn_id='gold_dw')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        logging.info("Starting dim_date calculated columns update")
        
        # Get current date
        current_date = datetime.now().date()
        
        # Update calculated columns based on current date
        update_query = """
        UPDATE dim_date 
        SET 
            day_lag = (CURRENT_DATE - date_string::date),
            today = CASE WHEN date_string::date = CURRENT_DATE THEN 1 ELSE 0 END,
            yesterday = CASE WHEN date_string::date = CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END,
            prev_7_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '7 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_14_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '14 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_30_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '30 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_45_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '45 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_60_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '60 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_90_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '90 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_180_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '180 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_270_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '270 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            prev_360_days = CASE WHEN date_string::date >= CURRENT_DATE - INTERVAL '360 days' AND date_string::date < CURRENT_DATE THEN 1 ELSE 0 END,
            persian_month_lag = (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM date_string::date)) * 12 + (EXTRACT(MONTH FROM CURRENT_DATE) - EXTRACT(MONTH FROM date_string::date)),
            persian_week_lag = (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM date_string::date)) * 52 + (EXTRACT(WEEK FROM CURRENT_DATE) - EXTRACT(WEEK FROM date_string::date)),
            persian_year_lag = EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM date_string::date),
            persian_shifted_week_lag = (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM date_string::date)) * 52 + (EXTRACT(WEEK FROM CURRENT_DATE) - EXTRACT(WEEK FROM date_string::date)) + 1,
            is_last_month_mtd = CASE 
                WHEN EXTRACT(MONTH FROM date_string::date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') 
                AND EXTRACT(YEAR FROM date_string::date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')
                THEN true 
                ELSE false 
            END,
            modify_date = CURRENT_TIMESTAMP
        WHERE date_string IS NOT NULL
        """
        
        cursor.execute(update_query)
        updated_rows = cursor.rowcount
        conn.commit()
        
        logging.info(f"Successfully updated {updated_rows} rows in dim_date table")
        
        # Log some statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                SUM(today) as today_count,
                SUM(yesterday) as yesterday_count,
                SUM(prev_7_days) as prev_7_days_count,
                SUM(prev_30_days) as prev_30_days_count
            FROM dim_date
        """)
        
        stats = cursor.fetchone()
        logging.info(f"Statistics - Total: {stats[0]}, Today: {stats[1]}, Yesterday: {stats[2]}, Prev7Days: {stats[3]}, Prev30Days: {stats[4]}")
        
    except Exception as e:
        logging.error(f"Error updating dim_date calculated columns: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def verify_dim_date_data(**context):
    """Verify the dim_date data integrity"""
    
    pg_hook = PostgresHook(postgres_conn_id='gold_dw')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        logging.info("Verifying dim_date data integrity")
        
        # Check for data quality issues
        checks = [
            ("Total rows", "SELECT COUNT(*) FROM dim_date"),
            ("Null date_string", "SELECT COUNT(*) FROM dim_date WHERE date_string IS NULL"),
            ("Invalid dates", "SELECT COUNT(*) FROM dim_date WHERE date_string::date < '1900-01-01' OR date_string::date > '2100-12-31'"),
            ("Today flag count", "SELECT COUNT(*) FROM dim_date WHERE today = 1"),
            ("Yesterday flag count", "SELECT COUNT(*) FROM dim_date WHERE yesterday = 1")
        ]
        
        for check_name, query in checks:
            cursor.execute(query)
            result = cursor.fetchone()[0]
            logging.info(f"{check_name}: {result}")
            
            if check_name == "Total rows" and result == 0:
                raise Exception("dim_date table is empty!")
        
        logging.info("dim_date data verification completed successfully")
        
    except Exception as e:
        logging.error(f"Error verifying dim_date data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

# Define tasks
update_calculated_columns = PythonOperator(
    task_id='update_dim_date_calculated_columns',
    python_callable=update_dim_date_calculated_columns,
    dag=dag
)

verify_data = PythonOperator(
    task_id='verify_dim_date_data',
    python_callable=verify_dim_date_data,
    dag=dag
)

# Set task dependencies
update_calculated_columns >> verify_data 