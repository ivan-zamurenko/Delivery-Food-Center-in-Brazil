import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def get_engine():
    load_dotenv("config.example.env")

    #! Database credentials
    db_config = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
    }

    """Create and return a SQLAlchemy engine."""
    connection_string = (
        f"postgresql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(connection_string, echo=True)
