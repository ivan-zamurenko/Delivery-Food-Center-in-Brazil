import pandas as pd
from sqlalchemy import text

#! Table names
ORDERS = "orders"
PAYMENTS = "payments"
DELIVERIES = "deliveries"
DRIVERS = "drivers"
STORES = "stores"
CHANNELS = "channels"
HUBS = "hubs"


def fetch_orders(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {ORDERS}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


def fetch_payments(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {PAYMENTS}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


def fetch_deliveries(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {DELIVERIES}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


def fetch_drivers(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {DRIVERS}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


def fetch_stores(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {STORES}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


def fetch_hubs(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {HUBS}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df


def fetch_channels(engine):
    """Fetch data from the database and return as a DataFrame."""
    query = text(f"SELECT * FROM {CHANNELS}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df
