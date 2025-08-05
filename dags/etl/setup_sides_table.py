from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
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
    'setup_sides_table',
    default_args=default_args,
    description='One-time setup of sides table with buy and sell values',
    schedule_interval=None,  # Manual trigger only - one time setup
    catchup=False,
    tags=['setup', 'sides'],
)

def populate_sides_table(**context):
    """
    Populate sides table with 'buy' and 'sell' values
    """
    try:
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Check if sides already exist
        check_query = "SELECT COUNT(*) as count FROM sides"
        result = gold_dw_hook.get_first(check_query)
        
        if result and result[0] > 0:
            logging.info("Sides table already has data")
            return "Sides table already populated"
        
        # Insert sides
        insert_query = """
        INSERT INTO sides (name, created_at, updated_at)
        VALUES 
            ('buy', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('sell', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (name) DO NOTHING
        """
        
        gold_dw_hook.run(insert_query)
        
        logging.info("Successfully populated sides table with 'buy' and 'sell'")
        return "Sides table populated successfully"
        
    except Exception as e:
        logging.error(f"Error populating sides table: {str(e)}")
        raise

def verify_sides_table(**context):
    """
    Verify that sides table has been populated correctly
    """
    try:
        # Connect to gold_dw database
        gold_dw_hook = PostgresHook(postgres_conn_id='gold_dw')
        
        # Get sides data
        select_query = "SELECT id, name, created_at, updated_at FROM sides ORDER BY name"
        sides_df = gold_dw_hook.get_pandas_df(select_query)
        
        if sides_df.empty:
            logging.error("Sides table is empty")
            raise Exception("Sides table is empty")
        
        logging.info(f"Found {len(sides_df)} sides in the table:")
        for _, row in sides_df.iterrows():
            logging.info(f"  - ID: {row['id']}, Name: {row['name']}, Created: {row['created_at']}, Updated: {row['updated_at']}")
        
        # Verify we have both 'buy' and 'sell'
        sides_list = sides_df['name'].tolist()
        if 'buy' not in sides_list or 'sell' not in sides_list:
            logging.error("Missing required sides: 'buy' and/or 'sell'")
            raise Exception("Missing required sides")
        
        logging.info("Sides table verification completed successfully")
        return "Sides table verification successful"
        
    except Exception as e:
        logging.error(f"Error verifying sides table: {str(e)}")
        raise

# Define tasks
populate_sides_task = PythonOperator(
    task_id='populate_sides_table',
    python_callable=populate_sides_table,
    dag=dag,
)

verify_sides_task = PythonOperator(
    task_id='verify_sides_table',
    python_callable=verify_sides_table,
    dag=dag,
)

# Define task dependencies
populate_sides_task >> verify_sides_task 