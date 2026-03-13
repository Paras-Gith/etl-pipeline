import time
import shutil
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_with_retry(filepath: str, table: str, retries: int = 3):
    from src.ingestion import detect_and_read
    from src.validator import validate_schema, load_schema
    from src.cleaner import clean_dataframe
    from src.loader import load_to_db

    for attempt in range(retries):
        try:
            df = detect_and_read(filepath)
            is_valid, errors = validate_schema(df, table)

            if not is_valid:
                raise ValueError(f"Schema errors: {errors}")

            schema = load_schema(table)
            df_clean = clean_dataframe(df, schema)
            load_to_db(df_clean, table)

            shutil.move(filepath, f"data/archive/{Path(filepath).name}")
            logging.info(f"✅ Successfully processed {filepath}")
            return True

        except Exception as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)

    shutil.move(filepath, f"data/failed/{Path(filepath).name}")
    logging.error(f"❌ File permanently failed: {filepath}")
    return False