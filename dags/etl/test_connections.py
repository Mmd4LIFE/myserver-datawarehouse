from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    'test_database_connections',
    default_args=default_args,
    description='Test database connections and basic operations',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'connections'],
)

def test_source_connection(**context):
    """
    Test connection to source database
    """
    try:
        # Test source database connection
        source_hook = PostgresHook(
            postgres_conn_id='source_crypto_bot',
            schema='crypto_bot'
        )
        
        # Test query
        result = source_hook.get_first("SELECT COUNT(*) FROM gold_price")
        count = result[0] if result else 0
        
        logging.info(f"Source database connection successful. Found {count} records in gold_price table")
        return f"Source connection OK - {count} records found"
        
    except Exception as e:
        logging.error(f"Error testing source connection: {str(e)}")
        raise

def test_dw_connection(**context):
    """
    Test connection to data warehouse
    """
    try:
        # Test data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='datawarehouse'
        )
        
        # Test query
        result = dw_hook.get_first("SELECT version()")
        version = result[0] if result else "Unknown"
        
        logging.info(f"Data warehouse connection successful. Version: {version}")
        return f"DW connection OK - {version}"
        
    except Exception as e:
        logging.error(f"Error testing DW connection: {str(e)}")
        raise

def create_test_table(**context):
    """
    Create a test table in data warehouse
    """
    try:
        # Data warehouse connection
        dw_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='datawarehouse'
        )
        
        # Create test table
        create_sql = """
        CREATE TABLE IF NOT EXISTS test_connection (
            id SERIAL PRIMARY KEY,
            test_message VARCHAR(200),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        dw_hook.run(create_sql)
        
        # Insert test record
        insert_sql = """
        INSERT INTO test_connection (test_message) 
        VALUES ('Connection test successful at ' || NOW())
        """
        
        dw_hook.run(insert_sql)
        
        logging.info("Test table created and populated successfully")
        return "Test table created successfully"
        
    except Exception as e:
        logging.error(f"Error creating test table: {str(e)}")
        raise

# Define tasks
test_source_task = PythonOperator(
    task_id='test_source_connection',
    python_callable=test_source_connection,
    dag=dag,
)

test_dw_task = PythonOperator(
    task_id='test_dw_connection',
    python_callable=test_dw_connection,
    dag=dag,
)

create_test_task = PythonOperator(
    task_id='create_test_table',
    python_callable=create_test_table,
    dag=dag,
)

# Define task dependencies
[test_source_task, test_dw_task] >> create_test_task 