
import psycopg
import pandas as pd
from etl.config.dbconfig import DB_CONFIG

def load(df: pd.DataFrame):
    try:
       
        df['date_key'] = pd.to_datetime(df['date_key']).dt.date

        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:

                # =================
                # DIM DATE
                # =================
                for d in df[["date_key"]].drop_duplicates().itertuples(index=False):
                    cur.execute("""
                        INSERT INTO dim_date (date_key, year, month, day)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (d.date_key, d.date_key.year, d.date_key.month, d.date_key.day))

                # =================
                # DIM CATEGORY
                # =================
                categories = df[['category_id','category_name']].drop_duplicates()
                cur.executemany("""
                    INSERT INTO dim_category (category_id, category_name)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, categories.itertuples(index=False, name=None))

                # =================
                # DIM USER
                # =================
                users = df[['user_id','user_name']].drop_duplicates()
                cur.executemany("""
                    INSERT INTO dim_user (user_id, user_name)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, users.itertuples(index=False, name=None))

                # =================
                # DIM PRODUCT
                # =================
                products = df[['product_id','product_name','category_id']].drop_duplicates()
                cur.executemany("""
                    INSERT INTO dim_product (product_id, product_name, category_key)
                    SELECT %s, %s, category_key
                    FROM dim_category
                    WHERE category_id=%s
                    ON CONFLICT DO NOTHING
                """, products.itertuples(index=False, name=None))

                # =================
                # FACT TABLE
                # =================
                fact_rows = [
                    (
                        row.date_key,
                        row.review_id,
                        row.rating,
                        row.rating_count,
                        row.discounted_price,
                        row.actual_price,
                        row.discount_amount,
                        row.user_id,
                        row.product_id
                    )
                    for row in df.itertuples(index=False)
                ]
                cur.executemany("""
                    INSERT INTO fact_product_review(
                        product_key, user_key, date_key,
                        review_id, rating, rating_count,
                        discounted_price, actual_price, discount_amount
                    )
                    SELECT
                        p.product_key,
                        u.user_key,
                        %s,
                        %s, %s, %s, %s, %s, %s
                    FROM dim_product p
                    JOIN dim_user u ON u.user_id=%s
                    WHERE p.product_id=%s
                """, fact_rows)

            conn.commit()
            print("✅ Data Loaded Successfully")

    except Exception as e:
        print("❌ Load Error:", e)
        raise