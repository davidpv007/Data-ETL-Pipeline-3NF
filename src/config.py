# src/db_config.py
import os
from dotenv import load_dotenv
import pathlib as pl
from pathlib import Path

load_dotenv()

class PostgresConfig:
    HOST = os.getenv("POSTGRES_HOST")
    PORT = os.getenv("POSTGRES_PORT")
    DB   = os.getenv("POSTGRES_DB")
    USER = os.getenv("POSTGRES_USER")
    PASS = os.getenv("POSTGRES_PASSWORD")

    @classmethod
    def sqlalchemy_url(cls) -> str:
        if cls.PASS:
            return f"postgresql+psycopg2://{cls.USER}:{cls.PASS}@{cls.HOST}:{cls.PORT}/{cls.DB}"
        return f"postgresql+psycopg2://{cls.USER}@{cls.HOST}:{cls.PORT}/{cls.DB}"



# Path to project root for the ldj project

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# CSV path for ETL script

csv_path_for_py = PROJECT_ROOT / "data" / "data_jobs.csv"

print("PROJECT_ROOT:", PROJECT_ROOT)
print("CSV PATH:", csv_path_for_py)
print("Exists?:", csv_path_for_py.exists())
