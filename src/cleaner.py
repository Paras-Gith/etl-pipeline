import pandas as pd

def clean_dataframe(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

    df.columns = df.columns.str.lower().str.replace(' ', '_')

    df.dropna(subset=schema['required_columns'], inplace=True)  
    df.fillna({'amount': 0, 'notes': 'N/A'}, inplace=True)    
    
    for col, dtype in schema['dtypes'].items():
        if col in df.columns:
            try:
                if dtype == 'datetime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif dtype == 'int':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                elif dtype == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                print(f"Type conversion failed for {col}: {e}")

    df.drop_duplicates(inplace=True)

    if 'status' in df.columns:
        df['status'] = df['status'].str.upper()

    return df