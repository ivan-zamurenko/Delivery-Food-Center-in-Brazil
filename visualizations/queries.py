import pandas as pd

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
    query = f"SELECT * FROM {ORDERS}"
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df
