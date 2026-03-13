import pandas as pd
import os
from pathlib import Path
import logging

logging.basicConfig(filename='logs/etl.log', level=logging.INFO)

def detect_and_read(filepath: str) -> pd.DataFrame:
    """Auto detect file type and read into dataframe."""
    ext = Path(filepath).suffix.lower()

    try:
        if ext == '.csv':
            df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
        elif ext in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath, engine='openpyxl')
        else:
            raise ValueError(f"Unspupported file type: {exit}")
        
        logging.info(f"Ingested{filepath} | Rows: {len(df)}")
        return df
    
    except Exception as e:
        logging.error(f"failed to ingest {filepath}: {e}")
        raise

def scan_input_folder(folder: str = "data/input") -> list:
    """watch folder for new files"""
    return [os.path.join(folder, f) for f in os.listdir(folder)
            if f.endswith(('.csv', '.xlsx', '.xls'))]
