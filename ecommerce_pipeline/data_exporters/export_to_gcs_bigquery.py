from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(df: pd.DataFrame, **kwargs):
    """
    Export data to GCS and BigQuery
    """
    # Configuration
    PROJECT_ID = 'ecommerce-data-project'  # Replace with your actual project ID
    DATASET_ID = 'ecommerce_data'
    BUCKET_NAME = 'ecommerce-data-project-bucket-new'
    
    # Set up credentials path
    current_dir = os.getcwd()
    project_root = os.path.dirname(current_dir)
    credentials_path = os.path.join(project_root, 'google_credentials.json')
    
    # List of all datasets to process
    dataset_files = [
        'olist_customers_dataset.csv',
        'olist_orders_dataset.csv',
        'olist_order_items_dataset.csv',
        'olist_order_payments_dataset.csv',
        'olist_products_dataset.csv',
        'olist_sellers_dataset.csv',
        'olist_order_reviews_dataset.csv',
        'product_category_name_translation.csv'
    ]
    
    # Initialize clients with explicit credentials
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bigquery_client = bigquery.Client.from_service_account_json(credentials_path)
    
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Process each dataset
    for file in dataset_files:
        try:
            # Read the CSV file
            file_path = os.path.join(project_root, 'raw_data', file)
            current_df = pd.read_csv(file_path)
            
            # Get table name (remove .csv and convert to snake case)
            table_name = file.replace('.csv', '').lower()
            
            # 1. Upload to GCS
            print(f"Uploading {table_name} to GCS...")
            csv_content = current_df.to_csv(index=False)
            blob = bucket.blob(f'raw_data/{table_name}.csv')
            blob.upload_from_string(csv_content)
            print(f"Uploaded {table_name} to GCS successfully")
            
            # 2. Load to BigQuery
            print(f"Loading {table_name} to BigQuery...")
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
            
            # Load data to BigQuery
            job = bigquery_client.load_table_from_dataframe(
                current_df, table_id, job_config=job_config
            )
            # Wait for the job to complete
            job.result()
            
            print(f"Loaded {len(current_df)} rows into {table_id}")
            
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")