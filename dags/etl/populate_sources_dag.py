from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from utils.telegram_alert import task_notify_success_legacy, task_notify_failure_legacy
import logging
import pandas as pd
import hashlib

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
    'populate_sources_table',
    default_args=default_args,
    description='Populate sources table from gold_price data',
    schedule_interval='31 * * * *',  # Run daily at 1 AM
    catchup=False,
    tags=['etl', 'sources', 'gold_price'],
)

def get_sources_from_gold_price(**context):
    """
    Extract sources from gold_price table in source database
    """
    try:
        # Connect to source database
        source_hook = PostgresHook(postgres_conn_id='source_crypto_bot')
        
        # Query to get unique sources
        query = """
        SELECT gp.source
        FROM gold_price AS gp
        GROUP BY gp.source
        ORDER BY MIN(gp.id);
        """
        
        # Execute query and get results
        sources_df = source_hook.get_pandas_df(query)
        
        logging.info(f"Found {len(sources_df)} unique sources")
        
        # Store results in XCom for next task
        context['task_instance'].xcom_push(key='sources_list', value=sources_df['source'].tolist())
        
        return sources_df['source'].tolist()
        
    except Exception as e:
        logging.error(f"Error extracting sources: {str(e)}")
        raise

def generate_color_for_source(source_name: str) -> str:
    """
    Generate a consistent hex color for a source name based on its hash
    """
    # Create a hash of the source name
    hash_object = hashlib.md5(source_name.encode())
    hex_dig = hash_object.hexdigest()
    
    # Take the first 6 characters and ensure it's a valid hex color
    color = "#" + hex_dig[:6]
    return color

def populate_sources_table(**context):
    """
    Populate sources table in gold_dw database with color assignment
    """
    try:
        # Get sources list from previous task
        sources_list = context['task_instance'].xcom_pull(task_ids='get_sources_from_gold_price', key='sources_list')
        
        if not sources_list:
            logging.warning("No sources found to populate")
            return "No sources to populate"
        
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # First, check if color column exists, if not add it
        try:
            color_check_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'sources' AND column_name = 'color'
            """
            color_exists = gold_dw_hook.get_pandas_df(color_check_query)
            
            if color_exists.empty:
                logging.info("Adding color column to sources table")
                add_color_column_query = """
                ALTER TABLE sources 
                ADD COLUMN color VARCHAR(7) DEFAULT NULL
                """
                gold_dw_hook.run(add_color_column_query)
                logging.info("Color column added successfully")
        except Exception as e:
            logging.warning(f"Could not add color column (might already exist): {str(e)}")
        
        # Get existing sources from gold_dw
        existing_sources_query = "SELECT name, color FROM sources WHERE deleted_at IS NULL"
        existing_sources_df = gold_dw_hook.get_pandas_df(existing_sources_query)
        existing_sources = existing_sources_df['name'].tolist() if not existing_sources_df.empty else []
        
        # Find new sources
        new_sources = [source for source in sources_list if source not in existing_sources]
        
        if not new_sources:
            logging.info("No new sources to add")
            # Update updated_at for existing sources and assign colors if missing
            for source in sources_list:
                existing_source = existing_sources_df[existing_sources_df['name'] == source]
                if not existing_source.empty and pd.isna(existing_source.iloc[0]['color']):
                    # Assign color to source that doesn't have one
                    color = generate_color_for_source(source)
                    update_color_query = """
                    UPDATE sources 
                    SET color = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE name = %s AND deleted_at IS NULL
                    """
                    gold_dw_hook.run(update_color_query, parameters=(color, source))
                    logging.info(f"Assigned color {color} to existing source: {source}")
                else:
                    update_query = """
                    UPDATE sources 
                    SET updated_at = CURRENT_TIMESTAMP 
                    WHERE name = %s AND deleted_at IS NULL
                    """
                    gold_dw_hook.run(update_query, parameters=(source,))
            return f"Updated {len(sources_list)} existing sources"
        
        # Insert new sources with colors
        insert_query = """
        INSERT INTO sources (name, color, created_at, updated_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (name) DO UPDATE SET 
            color = COALESCE(sources.color, EXCLUDED.color),
            updated_at = CURRENT_TIMESTAMP
        """
        
        for source in new_sources:
            color = generate_color_for_source(source)
            gold_dw_hook.run(insert_query, parameters=(source, color))
            logging.info(f"Added new source: {source} with color: {color}")
        
        # Update existing sources that might not have colors
        for source in existing_sources:
            existing_source = existing_sources_df[existing_sources_df['name'] == source]
            if not existing_source.empty and pd.isna(existing_source.iloc[0]['color']):
                color = generate_color_for_source(source)
                update_color_query = """
                UPDATE sources 
                SET color = %s, updated_at = CURRENT_TIMESTAMP 
                WHERE name = %s AND deleted_at IS NULL
                """
                gold_dw_hook.run(update_color_query, parameters=(color, source))
                logging.info(f"Assigned color {color} to existing source: {source}")
            else:
                update_query = """
                UPDATE sources 
                SET updated_at = CURRENT_TIMESTAMP 
                WHERE name = %s AND deleted_at IS NULL
                """
                gold_dw_hook.run(update_query, parameters=(source,))
        
        logging.info(f"Successfully processed {len(sources_list)} sources ({len(new_sources)} new, {len(existing_sources)} existing)")
        return f"Processed {len(sources_list)} sources ({len(new_sources)} new, {len(existing_sources)} existing)"
        
    except Exception as e:
        logging.error(f"Error populating sources table: {str(e)}")
        raise

def log_sources_summary(**context):
    """
    Log summary of sources table
    """
    try:
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Get summary
        summary_query = """
        SELECT 
            COUNT(*) as total_sources,
            COUNT(CASE WHEN deleted_at IS NULL THEN 1 END) as active_sources,
            COUNT(CASE WHEN deleted_at IS NOT NULL THEN 1 END) as deleted_sources,
            MAX(updated_at) as last_updated
        FROM sources
        """
        
        summary_df = gold_dw_hook.get_pandas_df(summary_query)
        
        if not summary_df.empty:
            summary = summary_df.iloc[0]
            logging.info(f"Sources Summary: Total={summary['total_sources']}, "
                        f"Active={summary['active_sources']}, "
                        f"Deleted={summary['deleted_sources']}, "
                        f"Last Updated={summary['last_updated']}")
        
        return "Summary logged successfully"
        
    except Exception as e:
        logging.error(f"Error logging summary: {str(e)}")
        raise

# Define tasks
extract_sources_task = PythonOperator(
    task_id='get_sources_from_gold_price',
    python_callable=get_sources_from_gold_price,
    dag=dag,
)

populate_sources_task = PythonOperator(
    task_id='populate_sources_table',
    python_callable=populate_sources_table,
    dag=dag,
)

log_summary_task = PythonOperator(
    task_id='log_sources_summary',
    python_callable=log_sources_summary,
    dag=dag,
)

# Notification tasks
notify_success_task = PythonOperator(
    task_id='notify_success_telegram',
    python_callable=task_notify_success_legacy,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    retries=0,
    dag=dag,
)

notify_failure_task = PythonOperator(
    task_id='notify_failure_telegram',
    python_callable=task_notify_failure_legacy,
    trigger_rule=TriggerRule.ONE_FAILED,
    retries=0,
    dag=dag,
)

# Define task dependencies
extract_sources_task >> populate_sources_task >> log_summary_task >> notify_success_task

[extract_sources_task, populate_sources_task, log_summary_task] >> notify_failure_task 