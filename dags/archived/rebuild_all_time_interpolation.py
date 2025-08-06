from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np

def all_time_interpolation():
    print("Starting all-time interpolation...")
    
    # Connect to database
    dw_hook = PostgresHook(
        postgres_conn_id='datawarehouse',
        schema='gold_dw'
    )
    
    # Step 1: Truncate the interpolated table
    print("Truncating fact_gold_price_interpolated...")
    truncate_sql = "TRUNCATE TABLE fact_gold_price_interpolated;"
    dw_hook.run(truncate_sql)
    
    # Step 2: Copy ALL data from fact_gold_price (not just last hour)
    print("Copying all data from fact_gold_price...")
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
    FROM fact_gold_price;
    """
    
    dw_hook.run(copy_sql)
    print("Copied all data from fact_gold_price")
    
    # Step 3: Get all minute-level time IDs for all time
    print("Getting all minute-level time IDs...")
    time_query = """
    SELECT time_id 
    FROM dim_time 
    WHERE second = 0
    ORDER BY time_id;
    """
    
    time_df = dw_hook.get_pandas_df(time_query)
    all_minutes = time_df['time_id'].tolist()
    print(f"Total minutes available: {len(all_minutes)}")
    
    # Step 4: Get all unique date_id values
    print("Getting all unique dates...")
    dates_query = """
    SELECT DISTINCT date_id 
    FROM fact_gold_price_interpolated 
    ORDER BY date_id;
    """
    
    dates_df = dw_hook.get_pandas_df(dates_query)
    all_dates = dates_df['date_id'].tolist()
    print(f"Total dates to process: {len(all_dates)}")
    
    # Step 5: Process each date separately to avoid memory issues
    total_interpolated = 0
    
    for date_id in all_dates:
        print(f"Processing date_id: {date_id}")
        
        # Get the actual time range for this date
        time_range_query = f"""
        SELECT MIN(rounded_time_id) as min_time, MAX(rounded_time_id) as max_time
        FROM fact_gold_price_interpolated
        WHERE date_id = {date_id};
        """
        
        time_range_df = dw_hook.get_pandas_df(time_range_query)
        if len(time_range_df) == 0 or time_range_df.iloc[0]['min_time'] is None:
            print(f"No data for date_id: {date_id}, skipping...")
            continue
            
        min_time = time_range_df.iloc[0]['min_time']
        max_time = time_range_df.iloc[0]['max_time']
        
        # Get minute-level time IDs for the actual data range
        date_minutes_query = f"""
        SELECT time_id 
        FROM dim_time 
        WHERE second = 0
            AND time_id >= {min_time}
            AND time_id <= {max_time}
        ORDER BY time_id;
        """
        
        date_time_df = dw_hook.get_pandas_df(date_minutes_query)
        date_minutes = date_time_df['time_id'].tolist()
        
        # Get data for this date
        data_query = f"""
        SELECT source_id, side_id, price, date_id, rounded_time_id, is_interpolated
        FROM fact_gold_price_interpolated
        WHERE date_id = {date_id}
        ORDER BY source_id, side_id, date_id, rounded_time_id;
        """
        
        data_df = dw_hook.get_pandas_df(data_query)
        
        if len(data_df) == 0:
            print(f"No data for date_id: {date_id}, skipping...")
            continue
        
        # Process each source-side-date combination
        results = []
        
        for (source_id, side_id, date_id), group in data_df.groupby(['source_id', 'side_id', 'date_id'], dropna=False):
            print(f"  Processing source_id={source_id}, side_id={side_id}, date_id={date_id}")
            
            # Get existing minutes for this combination
            existing_minutes = group['rounded_time_id'].tolist()
            
            # Find missing minutes for this date
            missing_minutes = [m for m in date_minutes if m not in existing_minutes]
            
            if missing_minutes:
                print(f"    Found {len(missing_minutes)} missing minutes")
                
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
        
        # Insert interpolated records for this date
        if results:
            interpolated_df = pd.DataFrame(results)
            
            print(f"  Inserting {len(interpolated_df)} interpolated records for date_id: {date_id}")
            
            for _, row in interpolated_df.iterrows():
                side_id_value = "NULL" if pd.isna(row['side_id']) else str(int(row['side_id']))
                insert_query = f"""
                INSERT INTO fact_gold_price_interpolated 
                (source_id, side_id, price, date_id, time_id, rounded_time_id, is_interpolated)
                VALUES ({row['source_id']}, {side_id_value}, {row['price']}, {row['date_id']}, {row['time_id']}, {row['rounded_time_id']}, true)
                """
                dw_hook.run(insert_query)
            
            total_interpolated += len(interpolated_df)
        else:
            print(f"  No interpolation needed for date_id: {date_id}")
    
    # Final verification
    verify_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN is_interpolated = true THEN 1 END) as interpolated_records,
        COUNT(CASE WHEN is_interpolated = false THEN 1 END) as original_records,
        COUNT(DISTINCT date_id) as unique_dates,
        COUNT(DISTINCT source_id) as unique_sources
    FROM fact_gold_price_interpolated
    """
    
    result = dw_hook.get_first(verify_query)
    print(f"\n=== FINAL RESULTS ===")
    print(f"Total records: {result[0]}")
    print(f"Original records: {result[2]}")
    print(f"Interpolated records: {result[1]}")
    print(f"Unique dates: {result[3]}")
    print(f"Unique sources: {result[4]}")
    print(f"Interpolation rate: {(result[1] / result[0] * 100):.2f}%")
    
    return f"All-time interpolation completed! Total: {result[0]}, Interpolated: {result[1]}"

if __name__ == "__main__":
    all_time_interpolation() 