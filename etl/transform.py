import pandas as pd
from datetime import datetime

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

   
    numeric_cols = ['discounted_price', 'actual_price', 'rating', 'rating_count']

    for col in numeric_cols:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(r'[^\d.]', '', regex=True),
            errors='coerce'
        ).fillna(0)

   
    df['category_name'] = df['category'].astype(str).str.split("|").str[0]

   
    df['category_id'] = pd.factorize(df['category_name'])[0] + 1


    df['discount_amount'] = df['actual_price'] - df['discounted_price']

  
    df['user_id'] = df['user_id'].fillna("UNKNOWN")
    df['user_name'] = df['user_name'].fillna("UNKNOWN")

 
    df['date_key'] = datetime.today().date()

    return df