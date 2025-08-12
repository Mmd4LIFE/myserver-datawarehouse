from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from utils.telegram_alert import task_notify_success_legacy, task_notify_failure_legacy, send_telegram_photos
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
import seaborn as sns
from PIL import Image, ImageDraw, ImageFont
import io
import os

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
    'cheap_expensive_chart',
    default_args=default_args,
    description='Create a chart of cheapest and expensive sources',
    schedule_interval='45 6 * * *', 
    catchup=False,
    tags=['report'],
)

def main_query(cheap_or_expensive: str):
    """
    Get the cheapest or expensive sources from the source database with colors
    """
    query = f"""
        WITH main_table AS (
            SELECT
                s.name AS source,
                s.color AS color,
                dt.time_id,
                AVG(price) AS price
            FROM fact_gold_price_interpolated AS fgpi
                JOIN dim_date dd
                    USING(date_id)
                JOIN dim_time AS dt
                    ON dt.time_id = fgpi.rounded_time_id
                JOIN sources AS s
                    ON s.id = fgpi.source_id
            WHERE dd.yesterday = 1
            GROUP BY s.id, s.name, s.color, dt.time_id
        ),

        mt2 AS (
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY time_id ORDER BY price) AS cheap,
                ROW_NUMBER() OVER(PARTITION BY time_id ORDER BY price DESC) AS expensive
            FROM main_table
            ORDER BY time_id, price
        )

        SELECT
            source,
            color,
            COUNT(time_id) AS minute_count,
            CONCAT(
                LPAD(CAST(FLOOR(ROUND(COUNT(*) / SUM(COUNT(*)) OVER(), 2) * 24) AS VARCHAR(2)), 2, '0'),
                ':',
                LPAD(CAST(ROUND((ROUND(COUNT(*) / SUM(COUNT(*)) OVER(), 2) * 24 - FLOOR(ROUND(COUNT(*) / SUM(COUNT(*)) OVER(), 2) * 24)) * 60) AS VARCHAR(2)), 2, '0')
            ) AS duration
        FROM mt2
        WHERE {cheap_or_expensive} = 1
        GROUP BY source, color
        ORDER BY minute_count DESC;
    """
    return query

def get_cheapest_and_expensive_sources(**context):
    """
    Get the cheapest and expensive sources from the source database
    """
    try:
        # Source database connection
        source_hook = PostgresHook(
            postgres_conn_id='datawarehouse',
            schema='gold_dw'
        )

        # Get the cheapest and expensive sources
        cheap_query = main_query('cheap')
        expensive_query = main_query('expensive')

        # Execute the queries
        cheap_df = source_hook.get_pandas_df(cheap_query)
        expensive_df = source_hook.get_pandas_df(expensive_query)

        logging.info(f"Cheap sources data: {len(cheap_df)} records")
        logging.info(f"Expensive sources data: {len(expensive_df)} records")

        # Store in XCom for the plotting task
        context['task_instance'].xcom_push(key='cheap_df', value=cheap_df.to_dict('records'))
        context['task_instance'].xcom_push(key='expensive_df', value=expensive_df.to_dict('records'))

        return "Data extraction completed successfully"
        
    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise

def create_clock_background(size=(2400, 2400)):
    """
    Create a clock background image, with 12 at the top and all numbers in correct clock positions.
    """
    fig, ax = plt.subplots(figsize=(size[0]/100, size[1]/100), dpi=100)
    
    # Create circle for clock face - MUCH BIGGER to match pie!
    circle = plt.Circle((0.5, 0.5), 0.75, color='white', alpha=0.8, linewidth=12, edgecolor='black')
    ax.add_patch(circle)
    
    # No rotation offset needed: standard clock math is angle = hour * 30 - 90
    # 12 at top (0 deg), 3 at right (90 deg), 6 at bottom (180 deg), 9 at left (270 deg)
    # So hour 1 is at 30 deg, hour 2 at 60 deg, etc.

    # Add hour markers
    for hour in range(12):
        # Correct clock positioning: 12 at top, numbers go clockwise
        # Mirror the positioning: start at 90 degrees (top) and go counterclockwise in math coordinates
        # but clockwise visually (since y-axis is flipped in display)
        angle = 90 - hour * 30  # Start at 90 degrees and subtract for clockwise movement
        x_outer = 0.5 + 0.74 * np.cos(np.radians(angle))
        y_outer = 0.5 + 0.74 * np.sin(np.radians(angle))
        x_inner = 0.5 + 0.65 * np.cos(np.radians(angle))
        y_inner = 0.5 + 0.65 * np.sin(np.radians(angle))
        
        ax.plot([x_inner, x_outer], [y_inner, y_outer], 'k-', linewidth=12)
        
        # Add hour numbers in correct positions - BIGGER!
        x_text = 0.5 + 0.58 * np.cos(np.radians(angle))
        y_text = 0.5 + 0.58 * np.sin(np.radians(angle))
        hour_label = 12 if hour == 0 else hour
        ax.text(x_text, y_text, str(hour_label), ha='center', va='center', 
                fontsize=48, fontweight='bold', fontfamily='serif')
    
    # Add minute markers
    for minute in range(60):
        if minute % 5 != 0:  # Skip hour positions
            angle = 90 - minute * 6  # Fix minute marker positioning to match hours
            x_outer = 0.5 + 0.74 * np.cos(np.radians(angle))
            y_outer = 0.5 + 0.74 * np.sin(np.radians(angle))
            x_inner = 0.5 + 0.70 * np.cos(np.radians(angle))
            y_inner = 0.5 + 0.70 * np.sin(np.radians(angle))
            
            ax.plot([x_inner, x_outer], [y_inner, y_outer], 'k-', linewidth=6)
    
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect('equal')
    ax.axis('off')
    
    return fig, ax
def plot_chart(**context):
    """
    Create pie charts on clock backgrounds for cheapest and expensive sources
    """
    try:
        # Set beautiful font for all text elements
        plt.rcParams.update({
            'font.family': 'serif',
            'font.serif': ['Times New Roman', 'DejaVu Serif', 'serif'],
            'font.weight': 'normal'
        })
        # Get data from XCom
        cheap_data = context['task_instance'].xcom_pull(task_ids='get_data_task', key='cheap_df')
        expensive_data = context['task_instance'].xcom_pull(task_ids='get_data_task', key='expensive_df')
        
        if not cheap_data or not expensive_data:
            logging.error("No data received from previous task")
            raise ValueError("No data available for plotting")
        
        # Convert back to DataFrames
        cheap_df = pd.DataFrame(cheap_data)
        expensive_df = pd.DataFrame(expensive_data)
        
        logging.info(f"Plotting cheap sources: {len(cheap_df)} records")
        logging.info(f"Plotting expensive sources: {len(expensive_df)} records")
        
        # Create output directory if it doesn't exist
        # Use external directory mounted to /opt/airflow/generated_charts
        output_dir = '/opt/airflow/generated_charts'
        os.makedirs(output_dir, exist_ok=True)
        
        # Get current datetime for filename
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Plot for cheapest sources
        if not cheap_df.empty:
            fig_cheap, ax_cheap = create_clock_background()
            
            # Prepare data for pie chart
            sizes = cheap_df['minute_count'].values
            colors = cheap_df['color'].fillna('#888888').values  # Default gray if no color
            labels = [f"{row['source']} ({row['duration']})" for _, row in cheap_df.iterrows()]
            
            # Create pie chart overlaid on clock (no labels on chart) - MUCH BIGGER!
            wedges, texts = ax_cheap.pie(
                sizes, 
                colors=colors,
                startangle=90,
                center=(0.5, 0.5),
                radius=0.75
            )
            
            # Add legend outside the clock - BIGGER!
            ax_cheap.legend(wedges, labels, title="Cheapest Sources", 
                           loc="center left", bbox_to_anchor=(1, 0, 0.5, 1),
                           fontsize=36, title_fontsize=42)
            
            plt.title('Cheapest Sources Distribution', 
                     fontsize=56, fontweight='bold', pad=80, fontfamily='serif')
            
            # Save cheapest chart
            cheap_filename = os.path.join(output_dir, f'cheapest_sources_clock_{current_datetime}.png')
            plt.savefig(cheap_filename, dpi=150, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            logging.info(f"Cheapest sources chart saved to: {cheap_filename}")
        
        # Plot for expensive sources
        if not expensive_df.empty:
            fig_expensive, ax_expensive = create_clock_background()
            
            # Prepare data for pie chart
            sizes = expensive_df['minute_count'].values
            colors = expensive_df['color'].fillna('#888888').values  # Default gray if no color
            labels = [f"{row['source']} ({row['duration']})" for _, row in expensive_df.iterrows()]
            
            # Create pie chart overlaid on clock (no labels on chart) - MUCH BIGGER!
            wedges, texts = ax_expensive.pie(
                sizes, 
                colors=colors,
                startangle=90,
                center=(0.5, 0.5),
                radius=0.75
            )
            
            # Add legend outside the clock - BIGGER!
            ax_expensive.legend(wedges, labels, title="Most Expensive Sources", 
                               loc="center left", bbox_to_anchor=(1, 0, 0.5, 1),
                               fontsize=36, title_fontsize=42)
            
            plt.title('Most Expensive Sources Distribution', 
                     fontsize=56, fontweight='bold', pad=80, fontfamily='serif')
            
            # Save expensive chart
            expensive_filename = os.path.join(output_dir, f'expensive_sources_clock_{current_datetime}.png')
            plt.savefig(expensive_filename, dpi=150, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            logging.info(f"Expensive sources chart saved to: {expensive_filename}")
        
        return f"Charts created successfully. Files saved in: {output_dir}"
        
    except Exception as e:
        logging.error(f"Error creating charts: {str(e)}")
        raise

def send_charts_to_telegram(**context):
    """
    Send the generated chart images to Telegram
    """
    try:
        # Get output directory and find the latest chart files
        output_dir = '/opt/airflow/generated_charts'
        
        # Find the most recent chart files (they should be from the current run)
        import glob
        cheap_files = sorted(glob.glob(os.path.join(output_dir, 'cheapest_sources_clock_*.png')), reverse=True)
        expensive_files = sorted(glob.glob(os.path.join(output_dir, 'expensive_sources_clock_*.png')), reverse=True)
        
        chart_files = []
        if cheap_files:
            chart_files.append(cheap_files[0])  # Most recent cheapest chart
        if expensive_files:
            chart_files.append(expensive_files[0])  # Most recent expensive chart
        
        # Check which files exist
        existing_files = [f for f in chart_files if os.path.exists(f)]
        
        if not existing_files:
            logging.warning("No chart files found to send to Telegram")
            return "No charts found to send"
        
        # Get execution date for caption
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Create caption with summary
        caption = f"""📊 Gold Price Source Analysis - {yesterday_date}"""
        
        # Send photos to Telegram
        logging.info(f"Sending {len(existing_files)} chart images to Telegram...")
        send_telegram_photos(existing_files, caption)
        
        return f"Successfully sent {len(existing_files)} chart images to Telegram"
        
    except Exception as e:
        logging.error(f"Error sending charts to Telegram: {str(e)}")
        raise

# Define tasks
get_data_task = PythonOperator(
    task_id='get_data_task',
    python_callable=get_cheapest_and_expensive_sources,
    on_success_callback=task_notify_success_legacy,
    on_failure_callback=task_notify_failure_legacy,
    dag=dag,
)

plot_task = PythonOperator(
    task_id='plot_task',
    python_callable=plot_chart,
    on_success_callback=task_notify_success_legacy,
    on_failure_callback=task_notify_failure_legacy,
    dag=dag,
)

send_telegram_task = PythonOperator(
    task_id='send_charts_to_telegram',
    python_callable=send_charts_to_telegram,
    on_success_callback=task_notify_success_legacy,
    on_failure_callback=task_notify_failure_legacy,
    dag=dag,
)

# Set task dependencies
get_data_task >> plot_task >> send_telegram_task