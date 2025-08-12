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
    schedule_interval='40 * * * *', 
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

def create_clock_background(size=(800, 800)):
    """
    Create a clock background image
    """
    fig, ax = plt.subplots(figsize=(size[0]/100, size[1]/100), dpi=100)
    
    # Create circle for clock face
    circle = plt.Circle((0.5, 0.5), 0.45, color='white', alpha=0.8, linewidth=2, edgecolor='black')
    ax.add_patch(circle)
    
    # Add hour markers
    for hour in range(12):
        angle = hour * 30 - 90  # Start from 12 o'clock
        x_outer = 0.5 + 0.4 * np.cos(np.radians(angle))
        y_outer = 0.5 + 0.4 * np.sin(np.radians(angle))
        x_inner = 0.5 + 0.35 * np.cos(np.radians(angle))
        y_inner = 0.5 + 0.35 * np.sin(np.radians(angle))
        
        ax.plot([x_inner, x_outer], [y_inner, y_outer], 'k-', linewidth=2)
        
        # Add hour numbers
        x_text = 0.5 + 0.32 * np.cos(np.radians(angle))
        y_text = 0.5 + 0.32 * np.sin(np.radians(angle))
        hour_label = 12 if hour == 0 else hour
        ax.text(x_text, y_text, str(hour_label), ha='center', va='center', 
                fontsize=12, fontweight='bold')
    
    # Add minute markers
    for minute in range(60):
        if minute % 5 != 0:  # Skip hour positions
            angle = minute * 6 - 90
            x_outer = 0.5 + 0.4 * np.cos(np.radians(angle))
            y_outer = 0.5 + 0.4 * np.sin(np.radians(angle))
            x_inner = 0.5 + 0.38 * np.cos(np.radians(angle))
            y_inner = 0.5 + 0.38 * np.sin(np.radians(angle))
            
            ax.plot([x_inner, x_outer], [y_inner, y_outer], 'k-', linewidth=0.5)
    
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
        output_dir = '/tmp/chart_outputs'
        os.makedirs(output_dir, exist_ok=True)
        
        # Plot for cheapest sources
        if not cheap_df.empty:
            fig_cheap, ax_cheap = create_clock_background()
            
            # Prepare data for pie chart
            sizes = cheap_df['minute_count'].values
            labels = [f"{row['source']}\n({row['duration']})" for _, row in cheap_df.iterrows()]
            colors = cheap_df['color'].fillna('#888888').values  # Default gray if no color
            
            # Create pie chart overlaid on clock
            wedges, texts, autotexts = ax_cheap.pie(
                sizes, 
                labels=labels,
                colors=colors,
                autopct='%1.1f%%',
                startangle=90,
                center=(0.5, 0.5),
                radius=0.25,
                textprops={'fontsize': 8}
            )
            
            # Enhance text visibility
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(7)
            
            plt.title('Cheapest Sources Distribution (Clock View)', 
                     fontsize=16, fontweight='bold', pad=20)
            
            # Save cheapest chart
            cheap_filename = os.path.join(output_dir, 'cheapest_sources_clock.png')
            plt.savefig(cheap_filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            logging.info(f"Cheapest sources chart saved to: {cheap_filename}")
        
        # Plot for expensive sources
        if not expensive_df.empty:
            fig_expensive, ax_expensive = create_clock_background()
            
            # Prepare data for pie chart
            sizes = expensive_df['minute_count'].values
            labels = [f"{row['source']}\n({row['duration']})" for _, row in expensive_df.iterrows()]
            colors = expensive_df['color'].fillna('#888888').values  # Default gray if no color
            
            # Create pie chart overlaid on clock
            wedges, texts, autotexts = ax_expensive.pie(
                sizes, 
                labels=labels,
                colors=colors,
                autopct='%1.1f%%',
                startangle=90,
                center=(0.5, 0.5),
                radius=0.25,
                textprops={'fontsize': 8}
            )
            
            # Enhance text visibility
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(7)
            
            plt.title('Most Expensive Sources Distribution (Clock View)', 
                     fontsize=16, fontweight='bold', pad=20)
            
            # Save expensive chart
            expensive_filename = os.path.join(output_dir, 'expensive_sources_clock.png')
            plt.savefig(expensive_filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close()
            logging.info(f"Expensive sources chart saved to: {expensive_filename}")
        
        # Create combined chart
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Cheapest sources subplot
        if not cheap_df.empty:
            # Create clock background for subplot
            circle1 = plt.Circle((0.5, 0.5), 0.45, color='white', alpha=0.8, linewidth=2, edgecolor='black')
            ax1.add_patch(circle1)
            
            # Add clock elements
            for hour in range(12):
                angle = hour * 30 - 90
                x_outer = 0.5 + 0.4 * np.cos(np.radians(angle))
                y_outer = 0.5 + 0.4 * np.sin(np.radians(angle))
                x_inner = 0.5 + 0.35 * np.cos(np.radians(angle))
                y_inner = 0.5 + 0.35 * np.sin(np.radians(angle))
                ax1.plot([x_inner, x_outer], [y_inner, y_outer], 'k-', linewidth=1)
                
                x_text = 0.5 + 0.32 * np.cos(np.radians(angle))
                y_text = 0.5 + 0.32 * np.sin(np.radians(angle))
                hour_label = 12 if hour == 0 else hour
                ax1.text(x_text, y_text, str(hour_label), ha='center', va='center', fontsize=8)
            
            # Pie chart
            sizes = cheap_df['minute_count'].values
            colors = cheap_df['color'].fillna('#888888').values
            
            wedges, _, autotexts = ax1.pie(
                sizes, 
                colors=colors,
                autopct='%1.1f%%',
                startangle=90,
                center=(0.5, 0.5),
                radius=0.2,
                textprops={'fontsize': 6}
            )
            
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
            
            ax1.set_xlim(0, 1)
            ax1.set_ylim(0, 1)
            ax1.set_aspect('equal')
            ax1.axis('off')
            ax1.set_title('Cheapest Sources', fontsize=14, fontweight='bold')
        
        # Expensive sources subplot
        if not expensive_df.empty:
            # Create clock background for subplot
            circle2 = plt.Circle((0.5, 0.5), 0.45, color='white', alpha=0.8, linewidth=2, edgecolor='black')
            ax2.add_patch(circle2)
            
            # Add clock elements
            for hour in range(12):
                angle = hour * 30 - 90
                x_outer = 0.5 + 0.4 * np.cos(np.radians(angle))
                y_outer = 0.5 + 0.4 * np.sin(np.radians(angle))
                x_inner = 0.5 + 0.35 * np.cos(np.radians(angle))
                y_inner = 0.5 + 0.35 * np.sin(np.radians(angle))
                ax2.plot([x_inner, x_outer], [y_inner, y_outer], 'k-', linewidth=1)
                
                x_text = 0.5 + 0.32 * np.cos(np.radians(angle))
                y_text = 0.5 + 0.32 * np.sin(np.radians(angle))
                hour_label = 12 if hour == 0 else hour
                ax2.text(x_text, y_text, str(hour_label), ha='center', va='center', fontsize=8)
            
            # Pie chart
            sizes = expensive_df['minute_count'].values
            colors = expensive_df['color'].fillna('#888888').values
            
            wedges, _, autotexts = ax2.pie(
                sizes, 
                colors=colors,
                autopct='%1.1f%%',
                startangle=90,
                center=(0.5, 0.5),
                radius=0.2,
                textprops={'fontsize': 6}
            )
            
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
            
            ax2.set_xlim(0, 1)
            ax2.set_ylim(0, 1)
            ax2.set_aspect('equal')
            ax2.axis('off')
            ax2.set_title('Most Expensive Sources', fontsize=14, fontweight='bold')
        
        # Add overall title and legend
        fig.suptitle('Gold Price Sources Distribution - Yesterday Analysis', 
                    fontsize=18, fontweight='bold', y=0.95)
        
        # Create legend
        all_sources = []
        all_colors = []
        
        if not cheap_df.empty:
            all_sources.extend(cheap_df['source'].tolist())
            all_colors.extend(cheap_df['color'].fillna('#888888').tolist())
        
        if not expensive_df.empty:
            for source, color in zip(expensive_df['source'], expensive_df['color'].fillna('#888888')):
                if source not in all_sources:
                    all_sources.append(source)
                    all_colors.append(color)
        
        # Add legend if we have sources
        if all_sources:
            legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color, label=source) 
                             for source, color in zip(all_sources, all_colors)]
            fig.legend(handles=legend_elements, loc='lower center', 
                      bbox_to_anchor=(0.5, 0.02), ncol=min(len(all_sources), 4),
                      fontsize=10)
        
        # Save combined chart
        combined_filename = os.path.join(output_dir, 'sources_comparison_clock.png')
        plt.savefig(combined_filename, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close()
        logging.info(f"Combined chart saved to: {combined_filename}")
        
        return f"Charts created successfully. Files saved in: {output_dir}"
        
    except Exception as e:
        logging.error(f"Error creating charts: {str(e)}")
        raise

def send_charts_to_telegram(**context):
    """
    Send the generated chart images to Telegram
    """
    try:
        # Define chart file paths
        output_dir = '/tmp/chart_outputs'
        chart_files = [
            os.path.join(output_dir, 'cheapest_sources_clock.png'),
            os.path.join(output_dir, 'expensive_sources_clock.png'),
            os.path.join(output_dir, 'sources_comparison_clock.png')
        ]
        
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