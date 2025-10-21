import sys
import os

#! Adjust the system path to include the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from analysis.db_connection import get_engine
from analysis.queries import fetch_orders, fetch_order_channel_distribution
from analysis.visualization import horizontal_bar_chart_order_channels_distribution


def main():
    # ? Set up database engine
    engine = get_engine()

    # ? Fetch ORDERS - CHANNELS distribution data
    orders_channels_distribution_data = fetch_order_channel_distribution(engine)
    horizontal_bar_chart_order_channels_distribution(orders_channels_distribution_data)


if __name__ == "__main__":
    main()
