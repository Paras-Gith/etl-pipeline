from sqlalchemy import create_engine, text
import pandas as pd
import yaml
from typing import Literal  

def get_engine(db_type: str = "postgresql"):
    with open("config/db_config.yaml") as f:
        cfg = yaml.safe_load(f)[db_type]

    if db_type == "postgresql":
        url = f"postgresql://{cfg['username']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    else:
        url = f"mysql+pymysql://{cfg['username']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"

    return create_engine(url)

def load_to_db(
    df: pd.DataFrame,
    table_name: str,
    db_type: str = "postgresql",
    if_exists: Literal["fail", "replace", "append"] = "append"
):
    engine = get_engine(db_type)
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000,
            method="multi"
        )
        print(f"Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"DB Load failed: {e}")
        raise
    finally:
        engine.dispose()