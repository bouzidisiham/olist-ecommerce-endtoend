import os
from pathlib import Path
from dotenv import load_dotenv

# Charger le .env si présent
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)

def env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

PG_USER = env("POSTGRES_USER")
PG_PWD  = env("POSTGRES_PASSWORD")
PG_DB   = env("POSTGRES_DB")
PG_HOST = env("POSTGRES_HOST")
PG_PORT = env("POSTGRES_PORT")
DATA_DIR = env("DATA_DIR")

PG_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
