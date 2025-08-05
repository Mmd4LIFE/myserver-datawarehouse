from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow import settings
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
    'setup_database_connections',
    default_args=default_args,
    description='Setup database connections for ETL processes',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'connections'],
)

def setup_source_connection(**context):
    """
    Setup connection to source database (localhost:5434)
    """
    try:
        session = settings.Session()
        
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'source_crypto_bot'
        ).first()
        
        if existing_conn:
            logging.info("Connection 'source_crypto_bot' already exists")
            return "Connection already exists"
        
        # Create new connection
        new_conn = Connection(
            conn_id='source_crypto_bot',
            conn_type='postgres',
            host='localhost',
            schema='crypto_bot',
            login='bot_user',
            password='bot_password_2024',
            port=5434,
            extra='{"sslmode": "prefer"}'
        )
        
        session.add(new_conn)
        session.commit()
        session.close()
        
        logging.info("Successfully created 'source_crypto_bot' connection")
        return "Connection created successfully"
        
    except Exception as e:
        logging.error(f"Error creating source connection: {str(e)}")
        raise

def setup_dw_connection(**context):
    """
    Setup connection to data warehouse database
    """
    try:
        session = settings.Session()
        
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'datawarehouse'
        ).first()
        
        if existing_conn:
            logging.info("Connection 'datawarehouse' already exists")
            return "Connection already exists"
        
        # Create new connection
        new_conn = Connection(
            conn_id='datawarehouse',
            conn_type='postgres',
            host='postgres-dw',  # Docker service name
            schema='datawarehouse',
            login='dw_user',
            password='DW_Secure_Pass_2024',
            port=5432,
            extra='{"sslmode": "prefer"}'
        )
        
        session.add(new_conn)
        session.commit()
        session.close()
        
        logging.info("Successfully created 'datawarehouse' connection")
        return "Connection created successfully"
        
    except Exception as e:
        logging.error(f"Error creating DW connection: {str(e)}")
        raise

# Define tasks
setup_source_task = PythonOperator(
    task_id='setup_source_connection',
    python_callable=setup_source_connection,
    dag=dag,
)

setup_dw_task = PythonOperator(
    task_id='setup_dw_connection',
    python_callable=setup_dw_connection,
    dag=dag,
)

# Define task dependencies (can run in parallel)
setup_source_task >> setup_dw_task 