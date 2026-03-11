
import os
import logging
from io import StringIO
from datetime import datetime
import requests
import pandas as pd


from etl.transform import transform
from etl.load import load

# ------------------------
# Logging Configuration
# ------------------------
logging.basicConfig(
    filename='etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ------------------------
# Extract Phase
# ------------------------
def extract_phase(url, backup_path="backups/"):
    """Fetch CSV from API and save local backup."""
    logging.info("🔹 Starting Extract Phase")
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Create backup folder
        os.makedirs(backup_path, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_path, f"raw_sales_{timestamp}.csv")
        with open(backup_file, "w") as f:
            f.write(response.text)

        logging.info(f"✅ Extracted {len(response.text.splitlines())} rows. Backup saved: {backup_file}")
        df = pd.read_csv(StringIO(response.text))
        return df
    except Exception as e:
        logging.error(f"❌ Extract Phase Failed: {e}")
        raise

# ------------------------
# Transform Phase
# ------------------------
def transform_phase(df):
    """Clean and prepare data."""
    logging.info("🔹 Starting Transform Phase")
    try:
        df_transformed = transform(df)
        logging.info(f"✅ Transform Phase Completed. Rows: {len(df_transformed)}")
        return df_transformed
    except Exception as e:
        logging.error(f"❌ Transform Phase Failed: {e}")
        raise

# ------------------------
# Load Phase
# ------------------------
def load_phase(df):
    """Load transformed data into Postgres."""
    logging.info("🔹 Starting Load Phase")
    try:
        load(df)
        logging.info("✅ Load Phase Completed. Database updated successfully.")
    except Exception as e:
        logging.error(f"❌ Load Phase Failed: {e}")
        raise

# ------------------------
# Orchestrator
# ------------------------
def run_pipeline():
    API_URL = "http://host.docker.internal:5000/sales/csv"
    try:
        df_raw = extract_phase(API_URL)
        df_clean = transform_phase(df_raw)
        load_phase(df_clean)
        print("✅ ETL Pipeline Completed Successfully")
    except Exception:
        print("❌ ETL Pipeline Failed. Check logs for details.")

if __name__ == "__main__":
    run_pipeline()