from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.dw_helpers import map_sources_batch, map_sides_batch

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

# DAG definition - One-time historical load
dag = DAG(
    'fact_gold_price_historical',
    default_args=default_args,
    description='One-time historical load of all gold price data until last hour',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['etl', 'gold_price', 'fact_table', 'historical', 'one_time'],
)

def extract_historical_gold_price_data(**context):
    """
    Extract all historical gold price data from source database until last hour
    """
    try:
        # Source database connection
        source_hook = PostgresHook(
            postgres_conn_id='source_crypto_bot',
            schema='crypto_bot'
        )
        
        # SQL query to extract ALL historical data until last hour
        query = """
        SELECT
            id,
            source,
            side,
            CASE
                WHEN currency = 'IRR' THEN ROUND(price / 10)
                WHEN currency = 'IRT' THEN ROUND(price)
            END AS price,
            CAST(TO_CHAR(created_at AT TIME ZONE 'Asia/Tehran', 'YYYYMMDD') AS INT) AS date_id,
            CAST(TO_CHAR(created_at AT TIME ZONE 'Asia/Tehran', 'HH24MISS') AS INT) AS time_id
        FROM gold_price
        WHERE created_at AT TIME ZONE 'Asia/Tehran' < DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran')
        ORDER BY 1;
        """
        
        # Execute query and get data
        df = source_hook.get_pandas_df(query)

        # Get unique source and side names for batch mapping
        unique_sources = df['source'].unique().tolist()
        unique_sides = df['side'].unique().tolist()
        
        # Batch map sources and sides to their IDs (much more efficient)
        sources_mapping = map_sources_batch(unique_sources, **context)
        sides_mapping = map_sides_batch(unique_sides, **context)
        
        # Map the IDs using the batch mappings
        df['source_id'] = df['source'].map(sources_mapping)
        df['side_id'] = df['side'].map(sides_mapping)
        
        # Convert to numeric and handle NaN values properly
        df['source_id'] = pd.to_numeric(df['source_id'], errors='coerce')
        df['side_id'] = pd.to_numeric(df['side_id'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['date_id'] = pd.to_numeric(df['date_id'], errors='coerce')
        df['time_id'] = pd.to_numeric(df['time_id'], errors='coerce')
        
        # Convert all NaN values to None for database insertion
        df = df.replace([float('nan'), pd.NA, pd.NaT], None)
        
        # Convert to proper types for database
        df['source_id'] = df['source_id'].astype('Int64')
        df['side_id'] = df['side_id'].astype('Int64')
        df['date_id'] = df['date_id'].astype('Int64')
        df['time_id'] = df['time_id'].astype('Int64')
        
        # Log the number of records extracted
        logging.info(f"Extracted {len(df)} historical records from source database")
        logging.info(f"Date range: {df['date_id'].min()} to {df['date_id'].max()}")
        
        # Store data in XCom for next task
        context['task_instance'].xcom_push(key='historical_gold_price_data', value=df.to_dict('records'))
        
        return f"Successfully extracted {len(df)} historical records"
        
    except Exception as e:
        logging.error(f"Error extracting historical data: {str(e)}")
        raise

def create_dw_table(**context):
    """
    Create fact_gold_price table in data warehouse if it doesn't exist
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Create table SQL
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_gold_price (
            id BIGINT PRIMARY KEY,
            source_id INTEGER,
            side_id INTEGER,
            price DECIMAL(15,2),
            date_id INTEGER,
            time_id INTEGER,
            etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute create table
        dw_hook.run(create_table_sql)
        logging.info("Created fact_gold_price table in data warehouse")
        
        return "Table created successfully"
        
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        raise

def load_historical_gold_price_data(**context):
    """
    Load extracted historical data into data warehouse
    """
    try:
        # Get data from previous task
        data = context['task_instance'].xcom_pull(task_ids='extract_historical_gold_price_data', key='historical_gold_price_data')
        
        if not data:
            logging.warning("No historical data to load")
            return "No historical data to load"
        
        # Convert back to DataFrame
        df = pd.DataFrame(data)
        
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Insert data into the table
        for index, row in df.iterrows():
            insert_sql = """
            INSERT INTO fact_gold_price (id, source_id, side_id, price, date_id, time_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                source_id = EXCLUDED.source_id,
                side_id = EXCLUDED.side_id,
                price = EXCLUDED.price,
                date_id = EXCLUDED.date_id,
                time_id = EXCLUDED.time_id,
                etl_timestamp = CURRENT_TIMESTAMP;
            """
            
            # Handle NaN values by converting them to None
            source_id = None if pd.isna(row['source_id']) else int(row['source_id'])
            side_id = None if pd.isna(row['side_id']) else int(row['side_id'])
            price = None if pd.isna(row['price']) else float(row['price'])
            date_id = None if pd.isna(row['date_id']) else int(row['date_id'])
            time_id = None if pd.isna(row['time_id']) else int(row['time_id'])
            
            dw_hook.run(insert_sql, parameters=(
                int(row['id']),
                source_id,
                side_id,
                price,
                date_id,
                time_id
            ))
        
        logging.info(f"Successfully loaded {len(df)} historical records into data warehouse")
        return f"Successfully loaded {len(df)} historical records"
        
    except Exception as e:
        logging.error(f"Error loading historical data: {str(e)}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_historical_gold_price_data',
    python_callable=extract_historical_gold_price_data,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_dw_table',
    python_callable=create_dw_table,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_historical_gold_price_data',
    python_callable=load_historical_gold_price_data,
    dag=dag,
)

# Define task dependencies
create_table_task >> extract_task >> load_task 