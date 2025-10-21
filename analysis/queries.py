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


def fetch_order_channel_distribution(engine):
    """Get the distribution or orders across channels."""
    query = f"""
    WITH channel_order_stats AS (
        SELECT 
            c.channel_name, 
            COUNT(o.order_id) AS total_orders,
            ROUND(COUNT(o.order_id) / SUM(COUNT(o.order_id)) OVER() * 100 , 2) AS percentage_orders
        FROM {ORDERS} o
        JOIN {CHANNELS} c ON o.channel_id = c.channel_id
        GROUP BY c.channel_name
    )
    SELECT *
    FROM channel_order_stats
    WHERE percentage_orders >= 0.5
    ORDER BY total_orders DESC
    """
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        print(df.columns)
        print(df.head())
    return df
