from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import numpy as np
from scipy.interpolate import interp1d

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

# DAG definition
dag = DAG(
    'fact_gold_price',
    default_args=default_args,
    description='Extract gold price data from source and load into DW',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False,
    tags=['etl', 'gold_price', 'fact_table'],
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
            END AS price,
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
        
        logging.info(f"Successfully loaded {len(df)} records into data warehouse")
        return f"Successfully loaded {len(df)} records"
        
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

def create_interpolated_table(**context):
    """
    Create fact_gold_price_interpolated table in data warehouse if it doesn't exist
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Drop existing table if it exists
        drop_table_sql = """
        DROP TABLE IF EXISTS fact_gold_price_interpolated CASCADE;
        """
        
        # Execute drop table
        dw_hook.run(drop_table_sql)
        logging.info("Dropped existing fact_gold_price_interpolated table")
        
        # Create interpolated table SQL
        create_table_sql = """
        CREATE TABLE fact_gold_price_interpolated (
            id SERIAL PRIMARY KEY,
            source_id INTEGER,
            side_id INTEGER,
            price DECIMAL(15,2),
            date_id INTEGER,
            time_id INTEGER,
            rounded_time_id INTEGER,
            is_interpolated BOOLEAN DEFAULT FALSE,
            etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Execute create table
        dw_hook.run(create_table_sql)
        logging.info("Created fact_gold_price_interpolated table in data warehouse")
        
        return "Interpolated table created successfully"
        
    except Exception as e:
        logging.error(f"Error creating interpolated table: {str(e)}")
        raise

def interpolate_gold_price_data(**context):
    """
    Interpolate missing gold price values and create complete minute-by-minute data
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Simple approach: First, copy all existing records from fact_gold_price for the last hour
        copy_existing_sql = """
        INSERT INTO fact_gold_price_interpolated 
        (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
        WITH last_hour_data AS (
            SELECT 
                source_id,
                side_id,
                price,
                date_id,
                time_id,
                CASE 
                    WHEN EXTRACT(SECOND FROM TO_TIMESTAMP(time_id::text, 'HH24MISS')) = 0 
                    THEN time_id 
                    ELSE time_id - EXTRACT(SECOND FROM TO_TIMESTAMP(time_id::text, 'HH24MISS'))::int
                END AS rounded_time_id,
                FALSE as is_interpolated
            FROM fact_gold_price 
            WHERE date_id = (
                SELECT date_id 
                FROM dim_date 
                WHERE date_string = (DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour')::DATE
            )
            AND time_id >= (
                SELECT time_id 
                FROM dim_time 
                WHERE fulltime = (DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour')::TIME
            ) 
            AND time_id < (
                SELECT time_id 
                FROM dim_time 
                WHERE fulltime = DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran')::TIME
            )
        )
        SELECT source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated FROM last_hour_data;
        """
        
        # Execute the copy
        dw_hook.run(copy_existing_sql)
        logging.info("Copied existing records from fact_gold_price")
        
        # Now add interpolated records for missing minutes
        # Get the count of existing records
        count_sql = "SELECT COUNT(*) FROM fact_gold_price_interpolated;"
        result = dw_hook.get_first(count_sql)
        existing_count = result[0] if result else 0
        
        logging.info(f"Copied {existing_count} existing records")
        
        # Step 3: Get the specific hour's minute-level time IDs (60 minutes for the hour)
        hour_range_query = """
        WITH main_table AS (
            SELECT
                'start' AS sn,
                (DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour')::TIME AS date_time
            UNION
            SELECT
                'end',
                DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran')::TIME
        ),
        hour_range AS (
            SELECT
                mt.sn,
                dt.time_id
            FROM main_table AS mt
                JOIN dim_time AS dt
                    ON dt.fulltime = mt.date_time
        )
        SELECT
            time_id AS rounded_time_id,
            minutefullstring24
        FROM dim_time
        WHERE second = 0
            AND time_id >= (SELECT time_id FROM hour_range WHERE sn = 'start') 
            AND time_id < (SELECT time_id FROM hour_range WHERE sn = 'end')
        ORDER BY time_id;
        """
        
        minute_time_df = dw_hook.get_pandas_df(hour_range_query)
        logging.info(f"Retrieved {len(minute_time_df)} minute-level time IDs for the hour")
        
        # Now add interpolated records for missing minutes using SQL
        interpolate_sql = """
        WITH hour_minutes AS (
            SELECT 
                time_id AS rounded_time_id
            FROM dim_time 
            WHERE second = 0
                AND time_id >= (
                    SELECT time_id 
                    FROM dim_time 
                    WHERE fulltime = (DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour')::TIME
                ) 
                AND time_id < (
                    SELECT time_id 
                    FROM dim_time 
                    WHERE fulltime = DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran')::TIME
                )
        ),
        source_side_combinations AS (
            SELECT DISTINCT source_id, side_id, date_id
            FROM fact_gold_price_interpolated
        ),
        missing_combinations AS (
            SELECT 
                ssc.source_id,
                ssc.side_id,
                ssc.date_id,
                hm.rounded_time_id
            FROM source_side_combinations ssc
            CROSS JOIN hour_minutes hm
            WHERE NOT EXISTS (
                SELECT 1 
                FROM fact_gold_price_interpolated fgpi 
                WHERE fgpi.source_id = ssc.source_id 
                    AND (fgpi.side_id IS NULL AND ssc.side_id IS NULL OR fgpi.side_id = ssc.side_id)
                    AND fgpi.date_id = ssc.date_id 
                    AND fgpi.rounded_time_id = hm.rounded_time_id
            )
        ),
        interpolated_values AS (
            SELECT 
                mc.source_id,
                mc.side_id,
                CASE 
                    WHEN mc.side_id IS NULL THEN
                        -- For NULL side_id, use average price for the source
                        (SELECT AVG(price) FROM fact_gold_price_interpolated WHERE source_id = mc.source_id AND date_id = mc.date_id)
                    ELSE
                        -- For non-NULL side_id, use average price for the source-side combination
                        (SELECT AVG(price) FROM fact_gold_price_interpolated WHERE source_id = mc.source_id AND side_id = mc.side_id AND date_id = mc.date_id)
                END AS price,
                mc.date_id,
                mc.rounded_time_id AS time_id,
                mc.rounded_time_id,
                TRUE as is_interpolated
            FROM missing_combinations mc
        )
        INSERT INTO fact_gold_price_interpolated 
        (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
        SELECT source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated FROM interpolated_values;
        """
        
        # Execute the interpolation
        dw_hook.run(interpolate_sql)
        logging.info("Added interpolated records for missing minutes")
        
        # Get final count
        final_count_sql = "SELECT COUNT(*) FROM fact_gold_price_interpolated;"
        final_result = dw_hook.get_first(final_count_sql)
        final_count = final_result[0] if final_result else 0
        
        interpolated_count = final_count - existing_count
        
        logging.info(f"Total records: {final_count}, Interpolated: {interpolated_count}")
        return f"Successfully processed {final_count} records (interpolated: {interpolated_count})"
        
    except Exception as e:
        logging.error(f"Error interpolating data: {str(e)}")
        raise

def validate_interpolated_data(**context):
    """
    Validate the interpolated data quality and provide statistics
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Get statistics about the interpolated data
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_interpolated = true THEN 1 END) as interpolated_records,
            COUNT(CASE WHEN is_interpolated = false THEN 1 END) as original_records,
            COUNT(DISTINCT source_id) as unique_sources,
            COUNT(DISTINCT side_id) as unique_sides,
            COUNT(DISTINCT date_id) as unique_dates,
            COUNT(DISTINCT rounded_time_id) as unique_minutes,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            STDDEV(price) as price_stddev
        FROM fact_gold_price_interpolated
        WHERE date_id = (
            SELECT date_id 
            FROM dim_date 
            WHERE date_string = (DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour')::DATE
        );
        """
        
        stats_df = dw_hook.get_pandas_df(stats_query)
        
        if not stats_df.empty:
            stats = stats_df.iloc[0]
            
            logging.info("=== INTERPOLATED DATA STATISTICS ===")
            logging.info(f"Total records: {stats['total_records']}")
            logging.info(f"Original records: {stats['original_records']}")
            logging.info(f"Interpolated records: {stats['interpolated_records']}")
            logging.info(f"Interpolation rate: {(stats['interpolated_records'] / stats['total_records'] * 100):.2f}%")
            logging.info(f"Unique sources: {stats['unique_sources']}")
            logging.info(f"Unique sides: {stats['unique_sides']}")
            logging.info(f"Unique dates: {stats['unique_dates']}")
            logging.info(f"Unique minutes: {stats['unique_minutes']}")
            logging.info(f"Average price: {stats['avg_price']:.2f}")
            logging.info(f"Price range: {stats['min_price']:.2f} - {stats['max_price']:.2f}")
            logging.info(f"Price standard deviation: {stats['price_stddev']:.2f}")
            
            # Validate data completeness
            expected_records_per_source_side = 60  # 60 minutes per hour
            expected_total = stats['unique_sources'] * stats['unique_sides'] * expected_records_per_source_side
            
            if stats['total_records'] == expected_total:
                logging.info("✅ Data completeness validation: PASSED")
            else:
                logging.warning(f"⚠️ Data completeness validation: Expected {expected_total}, got {stats['total_records']}")
            
            # Check for any null prices
            null_prices_query = """
            SELECT COUNT(*) as null_prices
            FROM fact_gold_price_interpolated
            WHERE price IS NULL
            AND date_id = (
                SELECT date_id 
                FROM dim_date 
                WHERE date_string = (DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour')::DATE
            );
            """
            
            null_prices_df = dw_hook.get_pandas_df(null_prices_query)
            null_prices = null_prices_df.iloc[0]['null_prices']
            
            if null_prices == 0:
                logging.info("✅ No null prices found")
            else:
                logging.warning(f"⚠️ Found {null_prices} null prices")
            
            return f"Validation completed. Total records: {stats['total_records']}, Interpolated: {stats['interpolated_records']}"
        else:
            logging.warning("No interpolated data found for validation")
            return "No data to validate"
            
    except Exception as e:
        logging.error(f"Error validating interpolated data: {str(e)}")
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

create_interpolated_table_task = PythonOperator(
    task_id='create_interpolated_table',
    python_callable=create_interpolated_table,
    dag=dag,
)

interpolate_task = PythonOperator(
    task_id='interpolate_gold_price_data',
    python_callable=interpolate_gold_price_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_interpolated_data',
    python_callable=validate_interpolated_data,
    dag=dag,
)

# Define task dependencies
create_table_task >> extract_task >> load_task >> create_interpolated_table_task >> interpolate_task >> validate_task 