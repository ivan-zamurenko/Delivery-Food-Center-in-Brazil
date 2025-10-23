# Delivery Time Optimization Script
# ---------------------------------
# Calculates average delivery times and order counts for each driver modal (e.g., bike, car).
# Loads data from CSVs, merges relevant tables, filters for successful deliveries, and outputs results to CSV.

import pandas as pd
import analysis.queries as queries
from analysis.db_connection import get_engine


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
    engine = get_engine()  # Not used here, but kept for future DB integration.

    # Read data from Postgres database (for testability and CI compatibility)
    orders_df = queries.fetch_orders(engine)
    deliveries_df = queries.fetch_deliveries(engine)
    drivers_df = queries.fetch_drivers(engine)

    # Merge orders, deliveries, and drivers into a single DataFrame
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

    # Filter for successful deliveries only
    filtered_df = merged_df[
        merged_df["delivery_status"].str.upper() == "DELIVERED"
    ].copy()

    # Calculate statistics and save results
    avg_delivery_time_df = calculate_delivery_time_statistics(filtered_df)
    avg_delivery_time_df.to_csv(
        "../results/task-b/driver_modal_delivery_time_statistics.csv", index=True
    )
    return avg_delivery_time_df


def calculate_delivery_time_statistics(filtered_df):
    """
    Calculate average delivery time and order count for each driver modal.
    - Adds a new column 'delivery_time_minutes' to the DataFrame.
    - Groups by 'driver_modal' and computes:
        * average_delivery_time_minutes: mean delivery time in minutes
        * delivery_orders_count: number of successful deliveries
    - Returns a sorted DataFrame with results.
    """
    # Calculate delivery time in minutes for each row
    filtered_df["delivery_time_minutes"] = (
        pd.to_datetime(filtered_df["order_moment_delivered"])
        - pd.to_datetime(filtered_df["order_moment_delivering"])
    ).dt.total_seconds() / 60

    # Group by driver modal and aggregate statistics
    return (
        filtered_df.groupby("driver_modal")
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
    get_delivery_time_data()
