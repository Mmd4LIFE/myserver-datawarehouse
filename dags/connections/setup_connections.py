from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow import settings
from airflow.utils.trigger_rule import TriggerRule
from utils.telegram_alert import task_notify_success, task_notify_failure
from utils.telegram_alert import task_notify_success_legacy, task_notify_failure_legacy
import logging
import os

from dotenv import load_dotenv
load_dotenv()

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'setup_database_connections',
    default_args=default_args,
    description='Setup database connections for ETL processes',
    schedule_interval=None, 
    catchup=False,
    tags=['setup', 'connections'],
)

def setup_source_connection(**context):
    """
    Setup connection to source database (goldmarketcap database)
    Note: Uses goldmarketcap-postgres-1 as host (Docker internal network)
    """
    try:
        session = settings.Session()
        
        # For Docker internal network, use container name instead of localhost
        # Port 5432 is the internal Docker port (5435 is only for external access)
        docker_host = 'goldmarketcap-postgres-1'
        docker_port = 5432
        
        # Check if connection exists
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'source_gold_db'
        ).first()
        
        if existing_conn:
            # Update existing connection to ensure it has correct values
            existing_conn.conn_type = 'postgres'
            existing_conn.host = docker_host
            existing_conn.schema = os.getenv('SOURCE_GOLD_POSTGRES_SCHEMA')
            existing_conn.login = os.getenv('SOURCE_GOLD_POSTGRES_LOGIN')
            existing_conn.password = os.getenv('SOURCE_GOLD_POSTGRES_PASSWORD')
            existing_conn.port = docker_port
            existing_conn.extra = '{"sslmode": "prefer"}'
            session.commit()
            session.close()
            logging.info(f"Connection 'source_gold_db' verified: {docker_host}:{docker_port}/{os.getenv('SOURCE_GOLD_POSTGRES_SCHEMA')}")
            return "Connection verified and updated successfully"
        
        # Create new connection if it doesn't exist
        new_conn = Connection(
            conn_id='source_gold_db',
            conn_type='postgres',
            host=docker_host,
            schema=os.getenv('SOURCE_GOLD_POSTGRES_SCHEMA'),
            login=os.getenv('SOURCE_GOLD_POSTGRES_LOGIN'),
            password=os.getenv('SOURCE_GOLD_POSTGRES_PASSWORD'),
            port=docker_port,
            extra='{"sslmode": "prefer"}'
        )
        
        session.add(new_conn)
        session.commit()
        session.close()
        
        logging.info(f"Successfully created 'source_gold_db' connection: {docker_host}:{docker_port}/{os.getenv('SOURCE_GOLD_POSTGRES_SCHEMA')}")
        return "Connection created successfully"
        
    except Exception as e:
        session.rollback()
        session.close()
        logging.error(f"Error setting up source connection: {str(e)}")
        raise

def setup_dw_connection(**context):
    """
    Setup connection to data warehouse database
    """
    try:
        session = settings.Session()
        
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'datawarehouse'
        ).first()
        
        if existing_conn:
            logging.info("Connection 'datawarehouse' already exists")
            return "Connection already exists"
        
        new_conn = Connection(
            conn_id='datawarehouse',
            conn_type='postgres',
            host=os.getenv('POSTGRES_HOST'),
            schema='datawarehouse',
            login=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
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

def setup_gold_dw_connection(**context):
    """
    Setup connection to gold_dw database
    """
    try:
        session = settings.Session()
        
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == 'gold_dw'
        ).first()
        
        if existing_conn:
            logging.info("Connection 'gold_dw' already exists")
            return "Connection already exists"
        
        new_conn = Connection(
            conn_id='gold_dw',
            conn_type='postgres',
            host=os.getenv('POSTGRES_HOST'),
            schema='gold_dw',
            login=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=5432,
            extra='{"sslmode": "prefer"}'
        )
        
        session.add(new_conn)
        session.commit()
        session.close()
        
        logging.info("Successfully created 'gold_dw' connection")
        return "Connection created successfully"
        
    except Exception as e:
        logging.error(f"Error creating gold_dw connection: {str(e)}")
        raise

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

setup_gold_dw_task = PythonOperator(
    task_id='setup_gold_dw_connection',
    python_callable=setup_gold_dw_connection,
    dag=dag,
)

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

setup_source_task >> [setup_dw_task, setup_gold_dw_task] >> notify_success_task
setup_source_task >> notify_failure_task