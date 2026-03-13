import json
import pandas as pd

def load_schema(table_name: str) -> dict:
    with open("config/schema_config.json") as f:
        return json.load(f)[table_name]
    
def validate_schema(df: pd.DataFrame, table_name: str) -> tuple[bool, list]:
    schema = load_schema(table_name)
    error = []


    missing = set(schema['required_columns']) - set(df.columns)
    if missing:
        error.append(f"missing columns: {missing}")
    

    for col, rules in schema.get('contraints', {}).items():
        if 'min' in rules and col in df.columns:
            voilations = df[df[col] < rules['min']]
            if not voilations.empty:
                error.append(f"{col} has {len(voilations)} value below min")

        if rules.get('unique') and col in df.columns:
            if df[col].duplicated().any():
                error.append(f"{col} has duplicate values")

    return len(error) == 0, error