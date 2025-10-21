from analysis.db_connection import get_engine
from analysis.queries import fetch_orders, fetch_order_channel_distribution
from analysis.visualization import horizontal_bar_chart_order_channels_distribution


def main():
    #! Database credentials
    db_config = {
        "user": "postgres",
        "password": "21091997",
        "host": "localhost",
        "port": "5432",
        "database": "food-delivery-brazil",
    }

    # ? Set up database engine
    engine = get_engine(db_config)

    # ? Fetch ORDERS - CHANNELS distribution data
    orders_channels_distribution_data = fetch_order_channel_distribution(engine)
    horizontal_bar_chart_order_channels_distribution(orders_channels_distribution_data)


if __name__ == "__main__":
    main()
