# src/db_config.py
import os
from dotenv import load_dotenv
import pathlib
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
