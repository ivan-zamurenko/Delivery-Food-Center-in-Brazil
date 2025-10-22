import sys
import os

#! Adjust the system path to include the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import run_profitability
from visualization import channel_profitability_heatmap


def main():
    #! Task A: Channel Profitability Heatmap
    channel_profitability_df = run_profitability.get_channel_profitability_data()
    channel_profitability_heatmap(channel_profitability_df)


if __name__ == "__main__":
    main()
