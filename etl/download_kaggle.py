# /opt/airflow/etl/download_kaggle.py
import os, json, stat
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset():
    DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
    DATASET = os.getenv("KAGGLE_DATASET", "olistbr/brazilian-ecommerce")
    KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
    KAGGLE_KEY = os.getenv("KAGGLE_KEY")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Crée ~/.kaggle/kaggle.json si absent
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    cred_path = kaggle_dir / "kaggle.json"
    if not cred_path.exists():
        if not KAGGLE_USERNAME or not KAGGLE_KEY:
            raise RuntimeError("KAGGLE_USERNAME/KAGGLE_KEY manquants")
        cred_path.write_text(json.dumps({"username": KAGGLE_USERNAME, "key": KAGGLE_KEY}))
        cred_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # chmod 600

    api = KaggleApi()
    api.authenticate()
    print(f"→ Downloading {DATASET} …")
    api.dataset_download_files(DATASET, path=str(DATA_DIR), unzip=True, quiet=False)
    print("✓ Kaggle dataset ready in", DATA_DIR)


if __name__ == "__main__":
    download_kaggle_dataset()
