# Gold Price Sources Clock Chart Implementation

## Overview
This implementation creates beautiful pie charts overlaid on clock backgrounds to visualize the distribution of cheapest and most expensive gold price sources throughout the day.

## Features Implemented

### 1. Color Management System
- **File Modified**: `dags/etl/populate_sources_dag.py`
- **Added Function**: `generate_color_for_source(source_name: str) -> str`
- **Database Enhancement**: Automatically adds `color` column to `sources` table
- **Color Assignment**: Uses MD5 hash of source name for consistent colors
- **Benefits**: Each source maintains the same color across all charts and time periods

### 2. Database Schema Enhancement
- **File Modified**: `dags/utils/dw_helpers.py`
- **Enhancement**: Updated `_load_sources_dimension()` to include color column
- **Query Update**: Now fetches `id`, `name`, and `color` from sources table
- **Compatibility**: Maintains backward compatibility with existing code

### 3. Comprehensive Chart Generation
- **File Modified**: `dags/report/cheap_expensive_chart.py`
- **New Functions**:
  - `create_clock_background()`: Creates detailed clock face with hour/minute markers
  - `plot_chart()`: Generates multiple chart variations
- **Chart Types Generated**:
  1. Individual cheapest sources chart
  2. Individual expensive sources chart  
  3. Combined comparison chart with legend

### 4. Enhanced Data Query
- **Query Enhancement**: Updated `main_query()` to include color information
- **Database Connection**: Uses proper Airflow PostgresHook connection
- **Data Flow**: Proper XCom usage for task data passing

### 5. Visualization Features
- **Clock Background**: Detailed 12-hour clock face with:
  - Hour markers (thick lines)
  - Minute markers (thin lines)
  - Numbered hours (1-12)
  - Clean white face with black markings
- **Pie Chart Overlay**: 
  - Consistent colors from database
  - Percentage labels
  - Source names with duration
  - Proper text visibility (white text with bold weight)
- **Professional Styling**:
  - High DPI (300) for crisp images
  - Proper titles and legends
  - Clean layouts with appropriate spacing

### 6. Dependencies Added
- **File Modified**: `requirements.txt`
- **New Dependencies**:
  - `matplotlib==3.7.2`
  - `seaborn==0.12.2` 
  - `pillow==10.0.0`
  - `numpy==1.24.3`

### 7. Testing Framework
- **File Created**: `test_chart_generation.py`
- **Test Coverage**:
  - Clock background generation
  - Individual pie charts
  - Combined charts
  - Sample data creation
- **Verified Working**: All tests pass in Docker environment

## Output Files Generated

When the DAG runs, it creates the following PNG files in `/tmp/chart_outputs/`:

1. **cheapest_sources_clock.png**: Individual chart for cheapest sources
2. **expensive_sources_clock.png**: Individual chart for most expensive sources  
3. **sources_comparison_clock.png**: Side-by-side comparison chart

## DAG Configuration

- **DAG Name**: `cheap_expensive_chart`
- **Schedule**: Every hour at minute 40 (`40 * * * *`)
- **Tasks**:
  1. `get_data_task`: Extracts yesterday's price data with colors
  2. `plot_task`: Generates all chart variations
- **Dependencies**: Tasks run sequentially with proper data passing

## Key Technical Improvements

1. **Consistent Colors**: Each source maintains the same color across all charts
2. **Professional Visualization**: Clock background provides intuitive time context
3. **High Quality Output**: 300 DPI PNG files suitable for reports
4. **Robust Error Handling**: Proper logging and exception handling
5. **Docker Compatibility**: Fully tested in Airflow Docker environment
6. **Scalable Design**: Easily extensible for additional chart types

## Usage Instructions

1. **First Run**: The system will automatically add color column to sources table
2. **Automatic Execution**: Charts generated hourly based on yesterday's data
3. **Output Location**: Check `/tmp/chart_outputs/` for generated images
4. **Manual Testing**: Use `test_chart_generation.py` for development testing

## Database Schema Changes

The `sources` table now includes:
```sql
-- New column added automatically
ALTER TABLE sources ADD COLUMN color VARCHAR(7) DEFAULT NULL;
```

Sources without colors are automatically assigned consistent hash-based colors.

## Performance Considerations

- Color assignment is one-time per source
- Chart generation is optimized for typical source counts (5-15 sources)
- Images are saved to temporary directory to avoid disk space issues
- Matplotlib figures are properly closed to prevent memory leaks

This implementation provides a complete, production-ready solution for visualizing gold price source distributions with beautiful clock-themed charts. 