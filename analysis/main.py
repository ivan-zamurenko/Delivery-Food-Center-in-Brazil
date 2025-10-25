import sys
import os

#! Adjust the system path to include the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import run_profitability, run_delivery_time_optimization
from visualization import (
    channel_profitability_heatmap,
    plot_average_delivery_time_by_driver_modal,
)


def main():
    #! Task A: Channel Profitability Heatmap
    channel_profitability_df = run_profitability.get_channel_profitability_data()
    channel_profitability_heatmap(channel_profitability_df)

    #! Task B: Delivery Time Optimization
    delivery_time_df = run_delivery_time_optimization.get_delivery_time_data()
    plot_average_delivery_time_by_driver_modal(delivery_time_df)


if __name__ == "__main__":
    main()
