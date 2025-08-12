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
from utils.telegram_alert import task_notify_success_legacy, task_notify_failure_legacy

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
    schedule_interval='35 * * * *',  # Every hour at minute 35
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
    Simple interpolation: Copy last hour data and interpolate missing minutes
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )
        
        # Step 1: Copy last hour data from fact_gold_price
        copy_sql = """
        INSERT INTO fact_gold_price_interpolated 
        (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
        SELECT
            source_id,
            side_id,
            price,
            date_id,
            time_id,
            CASE
                WHEN time_id % 100 = 0
                THEN time_id
                ELSE time_id - (time_id % 100)
            END AS rounded_time_id,
            FALSE as is_interpolated
        FROM fact_gold_price AS fgp
                JOIN dim_date AS dd
                    USING(date_id)
                JOIN dim_time AS dt
                    USING(time_id)
        WHERE CONCAT(dd.date_string, ' ', dt.minutefullstring24)::TIMESTAMP BETWEEN DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour'
            AND
            DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran');
        """
        
        dw_hook.run(copy_sql)
        logging.info("Copied last hour data from fact_gold_price")
        
        # Step 2: Get all minute-level time IDs for the hour
        start_hour = (datetime.now(timezone('Asia/Tehran')) - timedelta(hours=1)).hour
        time_query = f"""
        SELECT time_id 
        FROM dim_time 
        WHERE second = 0
            AND hour24 = {start_hour}
        ORDER BY time_id;
        """
        
        time_df = dw_hook.get_pandas_df(time_query)
        all_minutes = time_df['time_id'].tolist()
        
        # Step 3: Get existing data for the current hour being processed
        data_query = """
        SELECT fgpi.source_id, fgpi.side_id, fgpi.price, fgpi.date_id, fgpi.rounded_time_id, fgpi.is_interpolated
        FROM fact_gold_price_interpolated fgpi
        JOIN dim_date dd USING(date_id)
        JOIN dim_time dt ON dt.time_id = fgpi.rounded_time_id
        WHERE CONCAT(dd.date_string, ' ', dt.minutefullstring24)::TIMESTAMP BETWEEN DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran') - INTERVAL '1 hour'
            AND DATE_TRUNC('hour', CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tehran')
        ORDER BY fgpi.source_id, fgpi.side_id, fgpi.date_id, fgpi.rounded_time_id;
        """
        
        data_df = dw_hook.get_pandas_df(data_query)
        
        # Step 4: Process each source-side-date combination
        results = []
        
        for (source_id, side_id, date_id), group in data_df.groupby(['source_id', 'side_id', 'date_id'], dropna=False):
            # Get existing minutes for this combination
            existing_minutes = group['rounded_time_id'].tolist()
            
            # Find missing minutes
            missing_minutes = [m for m in all_minutes if m not in existing_minutes]
            
            if missing_minutes:
                # Get actual values for this combination
                actual_data = group[group['is_interpolated'] == False][['rounded_time_id', 'price']].sort_values('rounded_time_id')
                
                if len(actual_data) >= 2:
                    # Create interpolation function using actual values
                    actual_times = actual_data['rounded_time_id'].values
                    actual_prices = actual_data['price'].values
                    
                    # For each missing minute, find the two nearest actual values and interpolate linearly
                    for missing_minute in missing_minutes:
                        # Find the two nearest actual values
                        distances = np.abs(actual_times - missing_minute)
                        nearest_indices = np.argsort(distances)[:2]
                        
                        if len(nearest_indices) >= 2:
                            time1, time2 = actual_times[nearest_indices[0]], actual_times[nearest_indices[1]]
                            price1, price2 = actual_prices[nearest_indices[0]], actual_prices[nearest_indices[1]]
                            
                            # Linear interpolation: y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
                            if time2 != time1:
                                interpolated_price = price1 + (missing_minute - time1) * (price2 - price1) / (time2 - time1)
                            else:
                                interpolated_price = price1  # Same time, use the price
                            
                            # Add to results
                            results.append({
                                'source_id': source_id,
                                'side_id': side_id,
                                'date_id': date_id,
                                'time_id': missing_minute,
                                'rounded_time_id': missing_minute,
                                'price': interpolated_price,
                                'is_interpolated': True
                            })
        
        # Step 5: Insert interpolated records
        if results:
            interpolated_df = pd.DataFrame(results)
            
            for _, row in interpolated_df.iterrows():
                side_id_value = "NULL" if pd.isna(row['side_id']) else str(int(row['side_id']))
                insert_query = f"""
                INSERT INTO fact_gold_price_interpolated 
                (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
                VALUES ({row['source_id']}, {side_id_value}, {row['price']}, {row['date_id']}, {row['time_id']}, {row['rounded_time_id']}, true)
                """
                dw_hook.run(insert_query)
            
            logging.info(f"Inserted {len(interpolated_df)} interpolated records")
        else:
            logging.info("No missing values to interpolate")
        
        # Get final count
        final_count_sql = "SELECT COUNT(*) FROM fact_gold_price_interpolated;"
        final_result = dw_hook.get_first(final_count_sql)
        final_count = final_result[0] if final_result else 0
        
        logging.info(f"Total records: {final_count}")
        return f"Successfully processed {final_count} records"
        
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
create_table_task >> extract_task >> load_task >> create_interpolated_table_task >> interpolate_task >> validate_task >> notify_success_task
create_table_task >> notify_failure_task