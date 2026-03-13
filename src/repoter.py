import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from src.loader import get_engine

def generate_report():
    engine = get_engine("postgresql")
    
    df = pd.read_sql("SELECT * FROM orders WHERE date >= NOW() - INTERVAL '1 day'", engine)
    
    report = {
        'generated_at': datetime.now().isoformat(),
        'total_rows_processed': len(df),
        'total_revenue': df['amount'].sum(),
        'null_count': df.isnull().sum().to_dict(),
        'duplicate_count': df.duplicated().sum()
    }
    
   
    pd.DataFrame([report]).to_html(
        f"reports/report_{datetime.now().strftime('%Y%m%d')}.html",
        index=False
    )
    print(" Report generated!")