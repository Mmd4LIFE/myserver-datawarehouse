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

### 5. Enable Sources Population
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

## DAGs
- `setup_database_connections`: Creates Airflow connections
- `setup_sides_table`: One-time sides table population
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