# Data Warehouse with Gold DW

A data warehouse system with Airflow for ETL processes, focusing on gold price data management.

## Quick Setup

### 1. Start the Environment
```bash
docker compose up -d
```

### 2. Wait for Services to Start
```bash
# Check status
docker compose ps

# Check logs if needed
docker compose logs airflow-webserver
```

### 3. Setup Database Connections
- Go to Airflow UI: http://localhost:8082
- Trigger DAG: `setup_database_connections`

### 4. Setup Sides Table (One-time)
- Trigger DAG: `setup_sides_table`

### 5. Setup Time Dimension Table (One-time)
- Trigger DAG: `populate_dim_time` (or run manually if needed)

### 6. Enable Sources Population
- The `populate_sources_table` DAG runs daily at 1 AM
- It extracts sources from `gold_price` table and populates the `sources` table

## Database Structure

### gold_dw Database
- **Host**: localhost:5437 (external) / postgres-dw:5432 (internal)
- **User**: dw_user
- **Password**: DW_Secure_Pass_2024

### Tables
- **sources**: Gold price sources (auto-populated daily)
- **sides**: Buy/sell sides (static data)
- **dim_time**: Time dimension table (86,401 records - all seconds in a day)

### dim_time Table Structure
The `dim_time` table contains comprehensive time dimension data with 28 columns:
- **time_id**: Primary key (0-235959 representing HHMMSS format)
- **Hour24**: Hour in 24-hour format (0-23)
- **Minute**: Minute (0-59)
- **Second**: Second (0-59)
- **FullTime**: Time in HH:MM:SS format
- **AmPmStringEn**: AM/PM indicator
- **AmPmStringFa**: Persian AM/PM indicator
- Plus various formatted time strings and codes

## DW Helper Functions

### Core Functions
The `dags/utils/dw_helpers.py` module provides utility functions for data warehouse operations:

```python
from dw_helpers import detect_source, detect_side

# Detect source ID
source_id = detect_source('milli')  # Returns: 1
source_id = detect_source('taline') # Returns: 2

# Detect side ID
side_id = detect_side('buy')   # Returns: 1
side_id = detect_side('sell')  # Returns: 2
```

### Available Functions
- `detect_source(source_name)` - Get source ID by name
- `detect_side(side_name)` - Get side ID by name
- `get_all_sources()` - Get all sources with IDs
- `get_all_sides()` - Get all sides with IDs
- `validate_source_and_side(source_name, side_name)` - Validate both

### Usage Example
```python
# In your DAG
from dw_helpers import detect_source, detect_side

def create_dw_record(**context):
    source_id = detect_source('milli', **context)
    side_id = detect_side('buy', **context)
    
    # Create DW record with foreign keys
    dw_record = {
        'source_id': source_id,  # 1
        'side_id': side_id,      # 1
        'price': 100.50,
        'volume': 1000.0
    }
```

## DAGs
- `setup_database_connections`: Creates Airflow connections
- `setup_sides_table`: One-time sides table population
- `populate_dim_time`: One-time time dimension table population
- `populate_sources_table`: Daily sources extraction (1 AM)
- `gold_price_to_sheets`: Google Sheets integration
- `gold_price_etl`: ETL processes

## Ports
- **Airflow**: 8082
- **PostgreSQL**: 5437
- **Redis**: 6381

## Environment Variables
Set these in your environment or create a `.env` file:
```
POSTGRES_DB=datawarehouse
POSTGRES_USER=dw_user
POSTGRES_PASSWORD=DW_Secure_Pass_2024
POSTGRES_PORT_EXTERNAL=5437
AIRFLOW_UID=50000
AIRFLOW_GID=0
``` 