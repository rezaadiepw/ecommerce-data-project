import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import os
from typing import Dict

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data():
    """
    Load all Olist datasets from local CSV files
    """
    # Get the current directory and project root
    current_dir = os.getcwd()
    project_root = os.path.dirname(current_dir)
    
    # Return first dataset as main output
    first_file = 'olist_customers_dataset.csv'
    file_path = os.path.join(project_root, 'raw_data', first_file)
    df = pd.read_csv(file_path)
    print(f"Successfully loaded {first_file} with {len(df)} rows")
    
    return df

@test
def test_output(df):
    """
    Test if we received any data
    """
    assert isinstance(df, pd.DataFrame), "Output is not a pandas DataFrame"
    assert not df.empty, "DataFrame is empty"