from db_connection import get_engine
from queries import fetch_orders
from visualization import plot_orders


#! Database credentials
db_config = {
    "user": "postgres",
    "password": "21091997",
    "host": "localhost",
    "port": "5432",
    "database": "food-delivery-brazil",
}


engine = get_engine(db_config)
orders_df = fetch_orders(engine)
plot_orders(orders_df)
