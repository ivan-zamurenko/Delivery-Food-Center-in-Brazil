# Delivery Time Optimization Script
# ---------------------------------
# Calculates average delivery times and order counts for each driver modal (e.g., bike, car).
# Loads data from CSVs, merges relevant tables, filters for successful deliveries, and outputs results to CSV.

# This script is designed to analyze delivery performance by driver type (modal).
# It loads data from the database, merges relevant tables, filters for successful deliveries,
# calculates delivery times, and produces summary statistics and visualizations for business analysis.

import pandas as pd
import analysis.queries as queries
from analysis.db_connection import get_engine
from analysis.visualization import (
    plot_delivery_time_by_driver_modal,
)


def get_delivery_time_data():
    """
    Main function to fetch, merge, filter, and analyze delivery data.
    Steps:
    1. Load orders, deliveries, and drivers data from CSV files.
    2. Merge tables to create a unified DataFrame with all relevant columns.
    3. Filter for successful deliveries (where delivery_status == 'DELIVERED').
    4. Calculate average delivery time and order count per driver modal.
    5. Save results to CSV and return the DataFrame.
    """
    # Step 1: Get database engine (for future DB integration, currently not used)
    engine = get_engine()  # Not used here, but kept for future DB integration.

    # Step 2: Read data from Postgres database using queries module
    orders_df = queries.fetch_orders(engine)
    deliveries_df = queries.fetch_deliveries(engine)
    drivers_df = queries.fetch_drivers(engine)

    # Step 3: Merge orders, deliveries, and drivers into a single DataFrame
    # - orders: contains order timestamps
    # - deliveries: contains delivery status and driver info
    # - drivers: contains driver modal/type
    merged_df = (
        orders_df[["order_id", "order_moment_delivering", "order_moment_delivered"]]
        .merge(
            deliveries_df[["driver_id", "delivery_status", "delivery_order_id"]],
            left_on="order_id",
            right_on="delivery_order_id",
        )
        .merge(
            drivers_df[["driver_id", "driver_modal"]],
            left_on="driver_id",
            right_on="driver_id",
        )
    )

    # Step 4: Filter for successful deliveries only (status == 'DELIVERED')
    filtered_df = merged_df[
        merged_df["delivery_status"].str.upper() == "DELIVERED"
    ].copy()

    # Step 5: Calculate delivery time statistics and save results to CSV
    avg_delivery_time_df = calculate_delivery_time_statistics(filtered_df)
    avg_delivery_time_df.to_csv(
        "../results/task-b/driver_modal_delivery_time_statistics.csv", index=True
    )
    # Step 6: Return the summary DataFrame for further analysis or visualization
    return avg_delivery_time_df


def calculate_delivery_time_statistics(filtered_df, save_fig=True):
    """
    Calculate average delivery time and order count for each driver modal.
    - Adds a new column 'delivery_time_minutes' to the DataFrame.
    - Groups by 'driver_modal' and computes:
        * average_delivery_time_minutes: mean delivery time in minutes
        * delivery_orders_count: number of successful deliveries
    - Returns a sorted DataFrame with results.
    """
    # Step 1: Calculate delivery time in minutes for each row
    # - Convert timestamps to datetime
    # - Subtract delivering time from delivered time
    # - Convert result to minutes
    filtered_df["delivery_time_minutes"] = (
        pd.to_datetime(filtered_df["order_moment_delivered"])
        - pd.to_datetime(filtered_df["order_moment_delivering"])
    ).dt.total_seconds() / 60

    # Step 2: Drop rows with NaN delivery_time_minutes (missing or invalid timestamps)
    filtered_df = filtered_df.dropna(subset=["delivery_time_minutes"])

    # Step 3: Filter out extreme outliers in delivery time
    # - Only keep deliveries between 0 and 120 minutes (business logic)
    outlier_threshold = 120  # minutes
    outlier_low_threshold = 0  # minutes
    outlier_filtered_df = filtered_df[
        (filtered_df["delivery_time_minutes"] < outlier_threshold)
        & (filtered_df["delivery_time_minutes"] > outlier_low_threshold)
    ]

    # Step 4: Visualize delivery time distribution by driver modal
    if save_fig:
        plot_delivery_time_by_driver_modal(outlier_filtered_df)

    # Step 5: Group by driver modal and aggregate statistics
    # - Calculate mean delivery time and count of deliveries for each modal
    return (
        outlier_filtered_df.groupby("driver_modal")
        .agg(
            average_delivery_time_minutes=("delivery_time_minutes", "mean"),
            delivery_orders_count=("delivery_order_id", "count"),
        )
        .round(2)
        .reset_index()
        .sort_values(by="average_delivery_time_minutes", ascending=True)
    )


# Entry point for script execution
if __name__ == "__main__":
    # Run the main analysis and save results
    # This will produce summary statistics and visualizations for delivery performance
    get_delivery_time_data()
