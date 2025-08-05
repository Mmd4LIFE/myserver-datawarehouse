from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
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
    'gold_price_etl',
    default_args=default_args,
    description='Extract gold price data from source and load into DW',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False,
    tags=['etl', 'gold_price', 'crypto_bot'],
)

def extract_gold_price_data(**context):
    """
    Extract gold price data from source database
    """
    try:
        # Source database connection (localhost:5434)
        source_hook = PostgresHook(
            postgres_conn_id='source_crypto_bot',
            schema='crypto_bot'
        )
        
        # SQL query to extract data
        query = """
        SELECT
            id,
            source,
            side,
            CASE
                WHEN currency = 'IRR' THEN ROUND(price / 10)
                WHEN currency = 'IRT' THEN ROUND(price)
            END AS price_tmn,
            CAST(TO_CHAR(created_at AT TIME ZONE 'Asia/Tehran', 'YYYYMMDD') AS INT) AS date_id,
            CAST(TO_CHAR(created_at AT TIME ZONE 'Asia/Tehran', 'HH24MISS') AS INT) AS time_id
        FROM gold_price
        WHERE created_at AT TIME ZONE 'Asia/Tehran' BETWEEN DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour'
            AND
            DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran')
        ORDER BY 1;
        """
        
        # Execute query and get data
        df = source_hook.get_pandas_df(query)
        
        # Log the number of records extracted
        logging.info(f"Extracted {len(df)} records from source database")
        
        # Store data in XCom for next task
        context['task_instance'].xcom_push(key='gold_price_data', value=df.to_dict('records'))
        
        return f"Successfully extracted {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise

def create_dw_table(**context):
    """
    Create gold_price_raw table in data warehouse if it doesn't exist
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='datawarehouse'
        )
        
        # Create table SQL
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS gold_price_raw (
            id BIGINT PRIMARY KEY,
            source VARCHAR(100),
            side VARCHAR(50),
            price_tmn DECIMAL(15,2),
            date_id INTEGER,
            time_id INTEGER,
            etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute create table
        dw_hook.run(create_table_sql)
        logging.info("Created gold_price_raw table in data warehouse")
        
        return "Table created successfully"
        
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        raise

def load_gold_price_data(**context):
    """
    Load extracted data into data warehouse
    """
    try:
        # Get data from previous task
        data = context['task_instance'].xcom_pull(task_ids='extract_gold_price_data', key='gold_price_data')
        
        if not data:
            logging.warning("No data to load")
            return "No data to load"
        
        # Convert back to DataFrame
        df = pd.DataFrame(data)
        
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='datawarehouse'
        )
        
        # Insert data into the table
        for index, row in df.iterrows():
            insert_sql = """
            INSERT INTO gold_price_raw (id, source, side, price_tmn, date_id, time_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                source = EXCLUDED.source,
                side = EXCLUDED.side,
                price_tmn = EXCLUDED.price_tmn,
                date_id = EXCLUDED.date_id,
                time_id = EXCLUDED.time_id,
                etl_timestamp = CURRENT_TIMESTAMP;
            """
            
            dw_hook.run(insert_sql, parameters=(
                row['id'],
                row['source'],
                row['side'],
                row['price_tmn'],
                row['date_id'],
                row['time_id']
            ))
        
        logging.info(f"Successfully loaded {len(df)} records into data warehouse")
        return f"Successfully loaded {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_gold_price_data',
    python_callable=extract_gold_price_data,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_dw_table',
    python_callable=create_dw_table,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_gold_price_data',
    python_callable=load_gold_price_data,
    dag=dag,
)

# Define task dependencies
create_table_task >> extract_task >> load_task 