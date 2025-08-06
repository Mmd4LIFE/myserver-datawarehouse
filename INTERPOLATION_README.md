# Gold Price Data Interpolation

## Overview

The `fact_gold_price` DAG now includes an interpolation step that creates a complete minute-by-minute dataset by filling in missing values using linear interpolation.

## New Table: `fact_gold_price_interpolated`

### Structure
```sql
CREATE TABLE fact_gold_price_interpolated (
    id SERIAL PRIMARY KEY,
    source_id INTEGER,
    side_id INTEGER,
    price DECIMAL(15,2),
    date_id INTEGER,
    time_id INTEGER,
    rounded_time_id INTEGER,
    is_interpolated BOOLEAN DEFAULT FALSE,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, side_id, date_id, rounded_time_id)
);
```

### Key Features
- **Complete Coverage**: Every minute (1440 per day) for each source-side combination
- **Interpolation Flag**: `is_interpolated` field indicates whether the price was interpolated
- **Rounded Time**: Uses minute-level time IDs (seconds = 0) for consistent intervals
- **Unique Constraint**: Prevents duplicate records for the same source-side-date-minute combination

## Interpolation Process

### 1. Data Extraction
- Retrieves the last hour of data from `fact_gold_price`
- Uses the exact query provided in the requirements

### 2. Time Rounding
- Maps all time IDs to their corresponding minute-level rounded time IDs
- Uses the `dim_time` table where `second = 0` (1440 records per day)

### 3. Interpolation Logic
For each source-side combination:

1. **Create Complete Dataset**: Generate all 60 minute slots for the hour
2. **Merge with Actual Data**: Combine with existing data points
3. **Interpolate Missing Values**: Use linear interpolation between known points
4. **Handle Edge Cases**:
   - Single data point: Use the same value for all minutes
   - No data points: Skip the combination
   - Interpolation failure: Use forward/backward fill as fallback

### 4. Quality Assurance
- **Bounds Checking**: Clips interpolated prices to reasonable range (0-1,000,000)
- **Validation**: Ensures no null prices in final dataset
- **Statistics**: Provides detailed metrics on interpolation results

## DAG Flow

```
create_table_task >> extract_task >> load_task >> create_interpolated_table_task >> interpolate_task >> validate_task
```

### New Tasks Added

1. **`create_interpolated_table`**: Creates the interpolated table structure
2. **`interpolate_gold_price_data`**: Performs the interpolation
3. **`validate_interpolated_data`**: Validates and reports statistics

## Expected Results

### For Each Hour:
- **60 records per source-side combination** (complete minute coverage)
- **Interpolation rate**: Typically 70-90% depending on data availability
- **No null prices**: All missing values are filled
- **Data quality**: Prices within reasonable bounds

### Example Output:
```
=== INTERPOLATED DATA STATISTICS ===
Total records: 240
Original records: 45
Interpolated records: 195
Interpolation rate: 81.25%
Unique sources: 2
Unique sides: 2
Unique dates: 1
Unique minutes: 60
Average price: 1250.50
Price range: 1240.00 - 1260.00
Price standard deviation: 5.25
✅ Data completeness validation: PASSED
✅ No null prices found
```

## Dependencies

### New Python Packages
- `scipy==1.10.1`: For linear interpolation functionality (Python 3.8 compatible)
- `numpy`: For numerical operations (already included with pandas)

### Database Tables Required
- `fact_gold_price`: Source data
- `dim_time`: Time dimension for rounding
- `dim_date`: Date dimension for filtering

## Usage

The interpolation runs automatically as part of the hourly ETL process. The interpolated data is stored in `fact_gold_price_interpolated` and can be used for:

- **Analytics**: Complete time series analysis
- **Reporting**: Consistent minute-level reporting
- **Machine Learning**: Training models with complete datasets
- **Visualization**: Smooth charts without gaps

## Monitoring

Check the Airflow logs for:
- Interpolation statistics
- Data quality validation results
- Any warnings about insufficient data
- Performance metrics

## Troubleshooting

### Common Issues:
1. **No data to interpolate**: Check if source data exists for the hour
2. **Interpolation failures**: Check for extreme price values or insufficient data points
3. **Missing dependencies**: Ensure scipy is installed in the Airflow environment

### Validation Failures:
- **Data completeness**: Expected records = sources × sides × 60 minutes
- **Null prices**: Should be 0 after interpolation
- **Price bounds**: All prices should be positive and reasonable

## Testing

The interpolation logic has been tested with:
- ✅ **Normal interpolation**: 3 data points → 57 interpolated values
- ✅ **Single data point**: 1 data point → 59 interpolated values (same value)
- ✅ **Empty dataset**: No data → gracefully handled
- ✅ **Quality checks**: Interpolated values within reasonable bounds 