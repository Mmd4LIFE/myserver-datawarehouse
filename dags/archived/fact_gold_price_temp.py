from datetime import datetime, timedelta
from pytz import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import numpy as np
from scipy.interpolate import interp1d
from airflow.utils.trigger_rule import TriggerRule
from utils.telegram_alert import task_notify_success, task_notify_failure

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

# DAG definition - ARCHIVED/DISABLED
# dag = DAG(
#     'fact_gold_price_temp',
#     default_args=default_args,
#     description='Temporary DAG to fill missing 23:00-23:59 data for Aug 6-8',
#     schedule_interval=None,  # Manual execution only
#     catchup=False,
#     tags=['etl', 'gold_price', 'temp', 'interpolation'],
# )

def extract_gold_price_data(**context):
    """
    Extract gold price data from source database
    """
    try:
        # Source database connection (localhost:5434)
        source_hook = PostgresHook(
            postgres_conn_id='source_gold_db',
            schema='gold_price'
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
    Create fact_gold_price_interpolated table if it doesn't exist
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Create table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_gold_price_interpolated (
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
        logging.info("Ensured fact_gold_price_interpolated table exists")
        
        return "Table creation check completed successfully"
        
    except Exception as e:
        logging.error(f"Error creating interpolated table: {str(e)}")
        raise

def interpolate_gold_price_data(**context):
    """
    Fill missing 23:00-23:59 data for all source IDs except 1 for specific dates: Aug 6, 7, 8, 2025
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Get all source IDs except 1
        source_ids_result = dw_hook.get_pandas_df(
            "SELECT DISTINCT source_id FROM fact_gold_price_interpolated WHERE source_id != 1 ORDER BY source_id"
        )
        source_ids = source_ids_result['source_id'].tolist()
        logging.info(f"Processing source IDs: {source_ids}")
        
        # Target dates for filling missing 23:00-23:59 data
        target_dates = ['2025-08-06', '2025-08-07', '2025-08-08']
        total_inserted = 0
        
        for source_id in source_ids:
            logging.info(f"Processing source_id {source_id}")
            
            for date_str in target_dates:
                logging.info(f"Processing {date_str} for source_id {source_id} for hour 23:00-23:59")
                
                # Get date_id for the target date
                date_result = dw_hook.get_first(
                    "SELECT date_id FROM dim_date WHERE date_string = %s", 
                    parameters=[date_str]
                )
                if not date_result:
                    logging.warning(f"Date {date_str} not found in dim_date table")
                    continue
                date_id = date_result[0]
                
                # Calculate next day for hour 0 data
                next_day = (datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                
                # Get the last price from hour 22 of current date
                last_22_query = """
                SELECT fgpi.price, fgpi.side_id, dt.time_id
                FROM fact_gold_price_interpolated fgpi
                JOIN dim_date dd ON fgpi.date_id = dd.date_id
                JOIN dim_time dt ON fgpi.rounded_time_id = dt.time_id
                WHERE fgpi.source_id = %s
                AND dd.date_string = %s
                AND dt.hour24 = 22
                AND fgpi.is_interpolated = false
                ORDER BY dt.time_id DESC
                LIMIT 1;
                """
                
                last_22_result = dw_hook.get_first(last_22_query, parameters=[source_id, date_str])
                
                if not last_22_result:
                    logging.warning(f"No hour 22 data found for source_id {source_id} on {date_str}")
                    continue
                    
                last_22_price = float(last_22_result[0])
                last_22_side_id = last_22_result[1]
                last_22_time = last_22_result[2]
                
                # Get the first price from hour 0 of next date
                first_0_query = """
                SELECT fgpi.price, fgpi.side_id, dt.time_id
                FROM fact_gold_price_interpolated fgpi
                JOIN dim_date dd ON fgpi.date_id = dd.date_id
                JOIN dim_time dt ON fgpi.rounded_time_id = dt.time_id
                WHERE fgpi.source_id = %s
                AND dd.date_string = %s
                AND dt.hour24 = 0
                AND fgpi.is_interpolated = false
                ORDER BY dt.time_id ASC
                LIMIT 1;
                """
                
                first_0_result = dw_hook.get_first(first_0_query, parameters=[source_id, next_day])
                
                if not first_0_result:
                    logging.warning(f"No hour 0 data found for source_id {source_id} on next day after {date_str}")
                    continue
                    
                first_0_price = float(first_0_result[0])
                first_0_side_id = first_0_result[1]
                first_0_time = first_0_result[2]
                
                logging.info(f"Source {source_id}: Interpolating between: {last_22_time}({last_22_price}) -> {first_0_time}({first_0_price})")
                
                # Use the side_id from either reference point (prefer non-null)
                side_id_to_use = last_22_side_id if last_22_side_id is not None else first_0_side_id
                
                # Get all minute time_ids for hour 23
                minutes_23_df = dw_hook.get_pandas_df(
                    "SELECT time_id FROM dim_time WHERE second = 0 AND hour24 = 23 ORDER BY time_id"
                )
                minutes_23 = minutes_23_df['time_id'].tolist()
                
                # Calculate time range (accounting for day boundary)
                # Add 24 hours to first_0_time to make progression work
                end_time = first_0_time + 240000
                time_range = end_time - last_22_time
                price_range = first_0_price - last_22_price
                
                logging.info(f"Source {source_id}: Time range: {time_range}, Price range: {price_range}")
                
                # Insert interpolated records for all 60 minutes of hour 23
                inserted_count = 0
                for minute_time in minutes_23:
                    time_offset = minute_time - last_22_time
                    interpolated_price = last_22_price + (time_offset / time_range) * price_range
                    
                    # Insert the record
                    try:
                        if side_id_to_use is not None:
                            dw_hook.run("""
                                INSERT INTO fact_gold_price_interpolated 
                                (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
                                VALUES (%s, %s, %s, %s, %s, %s, true)
                            """, parameters=[
                                source_id,
                                int(side_id_to_use), 
                                round(interpolated_price, 2), 
                                date_id, 
                                minute_time, 
                                minute_time
                            ])
                        else:
                            # Handle NULL side_id case
                            dw_hook.run("""
                                INSERT INTO fact_gold_price_interpolated 
                                (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
                                VALUES (%s, NULL, %s, %s, %s, %s, true)
                            """, parameters=[
                                source_id,
                                round(interpolated_price, 2), 
                                date_id, 
                                minute_time, 
                                minute_time
                            ])
                        inserted_count += 1
                    except Exception as e:
                        logging.error(f"Error inserting source {source_id} {minute_time}: {e}")
                
                logging.info(f"Inserted {inserted_count} records for source_id {source_id} on {date_str}")
                total_inserted += inserted_count
        
        logging.info(f"Total interpolated records inserted: {total_inserted}")
        return f"Successfully processed {total_inserted} records"
        
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

# Define only the required tasks for interpolation testing - ARCHIVED/DISABLED
# interpolate_task = PythonOperator(
#     task_id='interpolate_gold_price_data',
#     python_callable=interpolate_gold_price_data,
#     dag=dag,
# )

# validate_task = PythonOperator(
#     task_id='validate_interpolated_data',
#     python_callable=validate_interpolated_data,
#     dag=dag,
# )

# Define task dependencies
# interpolate_task >> validate_task