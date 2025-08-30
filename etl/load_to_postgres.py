import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from pathlib import Path
from etl.config import PG_URL, DATA_DIR

FILES = {
    "olist_customers_dataset": "olist_customers_dataset.csv",
    "olist_geolocation_dataset": "olist_geolocation_dataset.csv",
    "olist_orders_dataset": "olist_orders_dataset.csv",
    "olist_order_items_dataset": "olist_order_items_dataset.csv",
    "olist_order_payments_dataset": "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset": "olist_order_reviews_dataset.csv",
    "olist_products_dataset": "olist_products_dataset.csv",
    "olist_sellers_dataset": "olist_sellers_dataset.csv",
}

DATE_COLS = {
    "olist_orders_dataset": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_order_reviews_dataset": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
}

def read_csv_safely(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing CSV: {path}")
    return pd.read_csv(path, low_memory=False)

def table_exists(conn, schema: str, table: str) -> bool:
    sql = text("select to_regclass(:qname) is not null as exists")
    qname = f"{schema}.{table}"
    return conn.execute(sql, {"qname": qname}).scalar()

if __name__ == "__main__":
    import traceback, os

    print(f"[ETL] DATA_DIR={DATA_DIR}")
    print(f"[ETL] PG_URL={PG_URL}")

    # 0) préflight
    try:
        print("[ETL] Files in DATA_DIR:", os.listdir(DATA_DIR))
    except Exception as e:
        print("[ETL] Cannot list DATA_DIR:", e, file=sys.stderr)

    engine = create_engine(PG_URL, pool_pre_ping=True)

    # 1) Schéma raw
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        print("[ETL] Ensured schema raw")

    errors = []
    # 2) Charge CSVs
    for table, filename in FILES.items():
        path = Path(DATA_DIR) / filename
        try:
            print(f"[ETL] Loading {filename} -> raw.{table}")
            df = read_csv_safely(path)

            # cast dates
            for c in DATE_COLS.get(table, []):
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")

            # 2.a Si la table existe déjà, on TRUNCATE (pas de DROP)
            with engine.begin() as conn:
                if table_exists(conn, "raw", table):
                    conn.execute(text(f"TRUNCATE TABLE raw.{table}"))
                    print(f"[ETL] TRUNCATE raw.{table}")
                # sinon pandas créera la table automatiquement avec append

            # 2.b Insert (append) pour éviter tout DROP
            df.to_sql(
                table,
                con=engine,
                schema="raw",
                if_exists="append",   # <== important
                index=False,
                chunksize=50000,
                method="multi",
            )
            print(f"[ETL] OK -> raw.{table} ({len(df):,} lignes)")
        except Exception as e:
            errors.append((table, str(e)))
            print(f"[ETL] ERROR on {table}: {e}", file=sys.stderr)
            traceback.print_exc()

    if errors:
        print("[ETL] Completed with errors:", errors, file=sys.stderr)
        sys.exit(1)

    print("✓ Ingestion completed.")
